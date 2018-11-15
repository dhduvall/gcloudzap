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
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/logging/logadmin"
	"github.com/dhduvall/gcloudzap"
	"github.com/golang/protobuf/ptypes/struct"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

// Integration testing requires Google Application Default Credentials which
// have read and write access to Stackdriver logs. If there is no
// authenticated gcloud CLI installation, this environment variable must be
// set: GOOGLE_APPLICATION_CREDENTIALS=path-to-credentials.json
//
// A project ID is also required: GCL_PROJECT_ID=test-project-id

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

// getEntry searches for the log entry specified by project, logID, and idfield,
// waiting for up to 20 seconds for it to show up, if necessary, and returns it.
func getEntry(ctx context.Context, t *testing.T, client *logadmin.Client, project, logID string, idfield zap.Field) *logging.Entry {
	var e *logging.Entry
	var err error
	insID := idfield.String
	for i := 0; i < 20; i++ {
		es := client.Entries(ctx,
			logadmin.Filter(fmt.Sprintf(`logName="projects/%s/logs/%s" `+
				`AND insertId="%s"`, project, testLogID, insID)),
			logadmin.NewestFirst())
		e, err = es.Next()
		if err == nil {
			return e
		}
		switch err {
		case iterator.Done:
			time.Sleep(1 * time.Second)
			continue
		default:
			t.Fatal(err)
		}
	}
	t.Fatal(fmt.Sprintf("Timed out waiting for %s/%s", logID, insID))
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

	e := getEntry(ctx, t, client, project, testLogID, idfield)
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

	e := getEntry(ctx, t, aClient, project, testLogID, idfield)
	fields := e.Payload.(*structpb.Struct).GetFields()
	assert.Len(fields, 2)
	assert.Equal("test_fields", fields["message"].GetStringValue())
	assert.Equal("value1", fields["field1"].GetStringValue())

	// Make sure that the local log is also correct and doesn't duplicate
	// fields.
	assertNoDups(t, assert, bufSinks["fields"])
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
	// This always fails, due to:
	// https://github.com/GoogleCloudPlatform/google-cloud-go/issues/1219
	// e := getEntry(ctx, t, aClient, project, testLogID, idfield)
	// assert.NotNil(e.SourceLocation)
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

func TestIntegration(t *testing.T) {
	project, _ := os.LookupEnv("GCL_PROJECT_ID")
	if project == "" {
		t.Fatal("GCL_PROJECT_ID is blank")
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

	t.Run("basic", func(t *testing.T) { test_basic(t, ctx, client, project) })
	t.Run("fields", func(t *testing.T) { test_fields(t, ctx, client, project) })
	t.Run("caller", func(t *testing.T) { test_caller(t, ctx, client, project) })
	t.Run("stack", func(t *testing.T) { test_stacktrace(t, ctx, client, project) })
}

func init() {
	rand.Seed(time.Now().UnixNano())
}
