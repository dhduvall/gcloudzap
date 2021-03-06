// +build integration

package gcloudzap_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/compute/metadata"
	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"github.com/dhduvall/gcloudzap"
	"github.com/golang/protobuf/ptypes/struct"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/api/iterator"
)

// Integration testing requires permissions to write and read Stackdriver logs,
// and uses Application Default Credentials.  This typically means setting the
// environment variable GOOGLE_APPLICATION_CREDENTIALS to the path to a file
// containing the credentials of a user or service account that has those
// permissions.
//
// If not running on a GCE VM, a the environment variable GCL_PROJECT_ID must
// also be set to the project ID.

const testLogID = "_gcloudzap_integration_test"

var (
	letters = []rune("abcdefghijklmnopqrstuvwxyz0123456789")

	bufSinks map[string]*bufferSink
)

// randString returns a random string of length n, derived from the characters
// in letters.
func randString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// idField returns a zap.Field representing a new random insertId field.
func idField() zap.Field {
	return zap.String(gcloudzap.InsertIDKey, randString(12))
}

type stack []uintptr

func (s *stack) StackTrace() errors.StackTrace {
	f := make([]errors.Frame, len(*s))
	for i := 0; i < len(f); i++ {
		f[i] = errors.Frame((*s)[i])
	}
	return f
}

type stackError struct {
	stack errors.StackTrace
}

func (e *stackError) StackTrace() errors.StackTrace {
	return e.stack
}

func (e *stackError) Error() string {
	return ""
}

func mkStackError() *stackError {
	var pcs [32]uintptr
	n := runtime.Callers(2, pcs[:])
	var st stack = pcs[0:n]
	return &stackError{st.StackTrace()}
}

func sourceField(err *stackError) zap.Field {
	return zap.Field{
		Key:       gcloudzap.CallerKey,
		Type:      gcloudzap.CallerType,
		Interface: err,
	}
}

// getEntry searches for the log entry specified by project, logID, and idfield,
// waiting for up to 20 seconds for it to show up, if necessary, and returns it.
func getEntry(ctx context.Context, t *testing.T, client *logadmin.Client, project, logID string, idfield zap.Field, fatal bool) *logging.Entry {
	var e *logging.Entry
	var err error
	insID := idfield.String
	now := time.Now().Add(-1 * time.Minute).Format("2006-01-02T15:04:05-07:00")
	for i := 0; i < 10; i++ {
		es := client.Entries(ctx,
			logadmin.Filter(fmt.Sprintf(`timestamp >= "%s" `+
				`AND logName="projects/%s/logs/%s" `+
				`AND insertId="%s"`, now, project, logID, insID)),
			logadmin.NewestFirst())
		e, err = es.Next()
		if err == nil {
			return e
		}
		switch err {
		case iterator.Done:
			time.Sleep(2 * time.Second)
			continue
		default:
			t.Fatal(err)
		}
	}
	if fatal {
		t.Fatal(fmt.Sprintf("Timed out waiting for %s/%s", logID, insID))
	}
	return nil
}

// bufferSink is an object providing an in-memory byte array sink for testing
// zap.  Zap implements this, but only in an internal package.
type bufferSink struct {
	bytes.Buffer
}

// Sync implements zapcore.WriteSyncer, part of zap.Sink
func (b *bufferSink) Sync() error {
	return nil
}

// Close implements zap.Sink
func (b *bufferSink) Close() error {
	return nil
}

// Simple test that logs an entry with a message as well as string and integer
// fields, and makes sure that this actually ended up in stackdriver with the
// right values.
func test_basic(t *testing.T, ctx context.Context, client *logadmin.Client, project string) {
	logger, err := gcloudzap.NewProduction(project, testLogID)
	if err != nil {
		t.Fatal(err)
	}
	sugar := logger.Sugar()

	l1 := sugar.With("foo", "bar").With("baz", 123)
	idfield := idField()
	l1.Infow("test_basic", idfield)

	e := getEntry(ctx, t, client, project, testLogID, idfield, true)
	fields := e.Payload.(*structpb.Struct).GetFields()
	if len(fields) != 3 {
		t.Errorf("entry has incorrect number of fields; expected %d, got %d", 3, len(fields))
	}
	if fields["message"].GetStringValue() != "test_basic" {
		t.Errorf("message has wrong value: %v", fields["message"].String())
	}
	if fields["foo"].GetStringValue() != "bar" {
		t.Errorf("foo has wrong value: %v", fields["foo"].String())
	}
	if fields["baz"].GetNumberValue() != 123 {
		t.Errorf("baz has wrong value: %v", fields["baz"].String())
	}
}

// assertNoDups streaming-decodes the byte array in bufSink into JSON and makes
// sure no fields have duplicate keys.
func assertNoDups(t *testing.T, assert *require.Assertions, bufSink *bufferSink) {
	jdec := json.NewDecoder(bufSink)
	tok, err := jdec.Token()
	assert.NoError(err)
	assert.Equal(json.Delim('{'), tok)
	found := make(map[string]interface{})
	for jdec.More() {
		key, err := jdec.Token()
		assert.NoError(err)
		assert.IsType("", key)
		value, err := jdec.Token()
		assert.NoError(err)
		if fval, ok := found[key.(string)]; ok {
			t.Errorf("Found duplicate key %q (values %v, %v)", key, fval, value)
		}
		found[key.(string)] = value
	}
}

// Test that adding a field through a zap.Option and building a gcloudzap logger
// with a config, a Google logging client, and that option emits what we expect.
func test_fields(t *testing.T, ctx context.Context, aClient *logadmin.Client, project string) {
	assert := require.New(t)

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"buffer://fields"}
	zapOptions := []zap.Option{
		zap.Fields(zap.String("field1", "value1")),
	}
	lClient, err := logging.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer lClient.Close()
	log, err := gcloudzap.New(config, lClient, testLogID, zapOptions...)
	if err != nil {
		t.Fatal(err)
	}
	slog := log.Sugar()

	idfield := idField()
	slog.Infow("test_fields", idfield)

	e := getEntry(ctx, t, aClient, project, testLogID, idfield, true)
	fields := e.Payload.(*structpb.Struct).GetFields()
	assert.Len(fields, 2)
	assert.Equal("test_fields", fields["message"].GetStringValue())
	assert.Equal("value1", fields["field1"].GetStringValue())

	// Make sure that the local log is also correct and doesn't duplicate
	// fields.
	assertNoDups(t, assert, bufSinks["fields"])
}

func test_logname(t *testing.T, ctx context.Context, aClient *logadmin.Client, project string) {
	assert := require.New(t)

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"buffer://logname"}

	lClient, err := logging.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer lClient.Close()

	log, err := gcloudzap.New(config, lClient, testLogID)
	if err != nil {
		t.Fatal(err)
	}
	slog := log.Sugar()

	idfield := idField()
	slog.Infow("test_logname", idfield)

	e := getEntry(ctx, t, aClient, project, testLogID, idfield, true)
	logName := strings.Join([]string{"projects", project, "logs", testLogID}, "/")
	assert.Equal(logName, e.LogName)

	slog = slog.Named("sublogger")
	idfield = idField()
	slog.Infow("test_logname2", idfield)

	subTestLogID := testLogID + ".sublogger"
	e = getEntry(ctx, t, aClient, project, subTestLogID, idfield, true)
	logName += ".sublogger"
	assert.Equal(logName, e.LogName)
}

// Make sure that if the configuration specifies adding the log call site, it
// shows up in the logs.
func test_caller(t *testing.T, ctx context.Context, aClient *logadmin.Client, project string) {
	assert := require.New(t)

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"buffer://caller"}

	lClient, err := logging.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer lClient.Close()

	log, err := gcloudzap.New(config, lClient, testLogID)
	if err != nil {
		t.Fatal(err)
	}
	slog := log.Sugar()

	idfield := idField()
	slog.Infow("test_caller", idfield)

	// Make sure that the local log contains a "caller" key with the right
	// information.
	m := make(map[string]interface{})
	err = json.Unmarshal(bufSinks["caller"].Bytes(), &m)
	if err != nil {
		t.Fatal(err)
	}
	caller, ok := m["caller"]
	assert.True(ok)
	assert.True(strings.HasPrefix(caller.(string), "gcloudzap/integration_test.go:"))

	// Make sure it's in the entry object, too.
	e := getEntry(ctx, t, aClient, project, testLogID, idfield, true)
	assert.NotNil(e.SourceLocation)
	assert.True(strings.HasSuffix(e.SourceLocation.File, "gcloudzap/integration_test.go"))
	assert.Equal("github.com/dhduvall/gcloudzap_test.test_caller", e.SourceLocation.Function)
	pSlice := strings.Split(e.SourceLocation.File, "/")
	p := strings.Join(pSlice[len(pSlice)-2:], "/")
	assert.Equal(caller.(string), fmt.Sprintf("%s:%d", p, e.SourceLocation.Line))
}

// Make sure that if we inject a call site into the payload, it makes it into
// Stackdriver in the SourceLocation.
func test_injectedCaller(t *testing.T, ctx context.Context, aClient *logadmin.Client, project string) {
	assert := require.New(t)

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"buffer://injected-caller"}

	lClient, err := logging.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer lClient.Close()

	log, err := gcloudzap.New(config, lClient, testLogID)
	if err != nil {
		t.Fatal(err)
	}
	slog := log.Sugar()

	idfield := idField()
	se := mkStackError()
	sourcefield := sourceField(se)
	slog.Infow("test_injected-caller", idfield, sourcefield)

	frame := se.stack[0]
	pc := uintptr(frame) - 1
	fn := runtime.FuncForPC(pc)
	_, seLine := fn.FileLine(pc)

	// Make sure that the local log contains a "caller" key with the right
	// information.
	m := make(map[string]interface{})
	err = json.Unmarshal(bufSinks["injected-caller"].Bytes(), &m)
	if err != nil {
		t.Fatal(err)
	}
	caller, ok := m["caller"]
	assert.True(ok)
	assert.True(strings.HasPrefix(caller.(string), "gcloudzap/integration_test.go:"))

	// Make sure it's in the entry object, too.
	e := getEntry(ctx, t, aClient, project, testLogID, idfield, true)
	assert.NotNil(e.SourceLocation)
	assert.True(strings.HasSuffix(e.SourceLocation.File, "gcloudzap/integration_test.go"))
	assert.Equal("github.com/dhduvall/gcloudzap_test.test_injectedCaller", e.SourceLocation.Function)
	assert.Equal(seLine, int(e.SourceLocation.Line))
	pSlice := strings.Split(e.SourceLocation.File, "/")
	p := strings.Join(pSlice[len(pSlice)-2:], "/")
	// We called slog.Infow() two lines after creating the error.
	assert.Equal(caller.(string), fmt.Sprintf("%s:%d", p, e.SourceLocation.Line+2))
}

// Test that the stacktrace field comes through, and that the level we set for
// stacktraces overrides the default set in New().
func test_stacktrace(t *testing.T, ctx context.Context, aClient *logadmin.Client, project string) {
	assert := require.New(t)

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"buffer://stack"}

	lClient, err := logging.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer lClient.Close()

	log, err := gcloudzap.New(config, lClient, testLogID,
		zap.AddStacktrace(zap.WarnLevel))
	if err != nil {
		t.Fatal(err)
	}
	slog := log.Sugar()

	idfield := idField()
	slog.Warnw("test_stacktrace", idfield)

	m := make(map[string]interface{})
	err = json.Unmarshal(bufSinks["stack"].Bytes(), &m)
	if err != nil {
		t.Fatal(err)
	}
	stack, ok := m["stacktrace"]
	assert.True(ok)
	assert.True(strings.HasPrefix(stack.(string),
		"github.com/dhduvall/gcloudzap_test.test_stacktrace"))
}

func test_skip(t *testing.T, ctx context.Context, aClient *logadmin.Client, project string) {
	assert := require.New(t)

	config := zap.NewProductionConfig()
	config.OutputPaths = []string{"buffer://skip"}

	lClient, err := logging.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer lClient.Close()

	log, err := gcloudzap.New(config, lClient, testLogID)
	if err != nil {
		t.Fatal(err)
	}
	slog := log.Sugar()

	idfield := idField()
	slog.Infow("test_logname", idfield, gcloudzap.SkipKey, nil)

	e := getEntry(ctx, t, aClient, project, testLogID, idfield, false)
	assert.Nil(e)

	idfield = idField()
	slog.Infow("test_logname", idfield, zapcore.Field{
		Key: gcloudzap.SkipKey, Type: zapcore.SkipType})

	e = getEntry(ctx, t, aClient, project, testLogID, idfield, false)
	assert.Nil(e)
}

func TestIntegration(t *testing.T) {
	project, _ := os.LookupEnv("GCL_PROJECT_ID")
	if project == "" {
		var err error
		project, err = metadata.ProjectID()
		if err != nil {
			t.Fatalf("Could not determine project ID; "+
				"please set $GCL_PROJECT_ID: %s", err)
		}
	}

	ctx := context.Background()
	client, err := logadmin.NewClient(ctx, project)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	bufSinks = make(map[string]*bufferSink, 0)
	zap.RegisterSink("buffer", func(u *url.URL) (zap.Sink, error) {
		name := u.Hostname()
		if _, ok := bufSinks[name]; ok {
			return nil, fmt.Errorf("Already created a buffer sink named %q", name)
		}
		bufSinks[name] = &bufferSink{}
		return bufSinks[name], nil
	})

	testCases := []struct {
		name  string
		tFunc func(*testing.T, context.Context, *logadmin.Client, string)
	}{
		{"basic", test_basic},
		{"fields", test_fields},
		{"logname", test_logname},
		{"caller", test_caller},
		{"injected-caller", test_injectedCaller},
		{"stack", test_stacktrace},
		{"skip", test_skip},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) { tc.tFunc(t, ctx, client, project) })
	}
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
