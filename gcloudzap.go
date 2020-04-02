/*
Package gcloudzap provides a zap logger that forwards entries to the Google
Stackdriver Logging service as structured payloads.

All zap.Logger instances created with this package are safe for concurrent
use.

Network calls (which are delegated to the Google Cloud Platform package) are
asynchronous and payloads are buffered. These benchmarks, on a MacBook Pro 2.4
GHz Core i5, are a loose approximation of latencies on the critical path for
the zapcore.Core implementation provided by this package.

	$ go test -bench . github.com/dhduvall/gcloudzap
	goos: darwin
	goarch: amd64
	pkg: github.com/dhduvall/gcloudzap
	BenchmarkCoreClone-4   	 2000000	       607 ns/op
	BenchmarkCoreWrite-4   	 1000000	      2811 ns/op


Zap docs: https://godoc.org/go.uber.org/zap

Stackdriver Logging docs: https://cloud.google.com/logging/docs/

*/
package gcloudzap // import "github.com/dhduvall/gcloudzap"

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"runtime"
	"sort"
	"sync"
	"time"

	gcl "cloud.google.com/go/logging"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	logpb "google.golang.org/genproto/googleapis/logging/v2"
)

const (
	// InsertIDKey is the payload field key to use to set the insertId field
	// in the LogEntry object.
	InsertIDKey = "logging.googleapis.com/insertId"

	// CallerKey is the payload field key to use to set the sourceLocation
	// field in the LogEntry object.
	CallerKey = "logging.googleapis.com/sourceLocation"
	// CallerType is the zap field type to use to set the sourceLocation
	// field in the LogEntry object.
	CallerType = zapcore.SkipType

	// SkipKey is the payload field key to use to tell the core not to write
	// the entry to Stackdriver.
	SkipKey = "logging.googleapis.com/skipLogging"
	// SkipType is the zap field type that ought to (though needn't) be used
	// with a field with key SkipKey.
	SkipType = zapcore.SkipType
)

var (
	// SkipField is a pre-constructed field that will skip being logged
	SkipField = zapcore.Field{Key: SkipKey, Type: SkipType}
)

func newClient(projectID string) (*gcl.Client, error) {
	if projectID == "" {
		return nil, newError("the provided projectID is empty")
	}

	return gcl.NewClient(context.Background(), projectID)
}

// NewDevelopment builds a development Logger that writes DebugLevel and above
// logs to standard error in a human-friendly format, as well as to
// Stackdriver using Application Default Credentials.
func NewDevelopment(projectID string, logID string) (*zap.Logger, error) {
	if logID == "" {
		return nil, fmt.Errorf("the provided logID is empty")
	}

	client, err := newClient(projectID)
	if err != nil {
		return nil, newError("creating Google Logging client: %v", err)
	}

	return New(zap.NewDevelopmentConfig(), client, logID)
}

// NewProduction builds a production Logger that writes InfoLevel and above
// logs to standard error as JSON, as well as to Stackdriver using Application
// Default Credentials.
func NewProduction(projectID string, logID string) (*zap.Logger, error) {
	if logID == "" {
		return nil, fmt.Errorf("the provided logID is empty")
	}

	client, err := newClient(projectID)
	if err != nil {
		return nil, newError("creating Google Logging client: %v", err)
	}

	return New(zap.NewProductionConfig(), client, logID)
}

// New creates a new zap.Logger which will write entries to Stackdriver in
// addition to the destination specified by the provided zap configuration.
func New(cfg zap.Config, client *gcl.Client, logID string, opts ...zap.Option) (*zap.Logger, error) {
	zl, err := cfg.Build()
	if err != nil {
		return nil, err
	}

	if client == nil {
		return nil, fmt.Errorf("The provided GCL client is nil")
	}

	// Here we translate all the members of a zap.Config into a zap.Option
	// array to pass to zap.New(), since otherwise the config passed in to
	// zl by cfg.Build() is lost when we grab its core; we basically copy
	// zap.Config.buildOptions().
	var nopts []zap.Option
	if cfg.Development {
		nopts = append(nopts, zap.Development())
	}

	if !cfg.DisableCaller {
		nopts = append(nopts, zap.AddCaller())
	}

	stackLevel := zap.ErrorLevel
	if cfg.Development {
		stackLevel = zap.WarnLevel
	}
	if !cfg.DisableStacktrace {
		nopts = append(nopts, zap.AddStacktrace(stackLevel))
	}

	if cfg.Sampling != nil {
		nopts = append(nopts, zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSampler(core, time.Second,
				int(cfg.Sampling.Initial), int(cfg.Sampling.Thereafter))
		}))
	}

	if len(cfg.InitialFields) > 0 {
		fs := make([]zap.Field, 0, len(cfg.InitialFields))
		keys := make([]string, 0, len(cfg.InitialFields))
		for k := range cfg.InitialFields {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fs = append(fs, zap.Any(k, cfg.InitialFields[k]))
		}
		nopts = append(nopts, zap.Fields(fs...))
	}

	// The user-supplied options must override our defaults
	opts = append(nopts, opts...)

	tee := Tee(zl.Core(), client, logID)
	return zap.New(tee, opts...), nil
}

// A Core implements zapcore.Core and writes entries to a Logger from the
// Google Cloud package.
//
// It's safe for concurrent use by multiple goroutines as long as it's not
// mutated after first use.
type Core struct {
	// Logger is a logging.Logger instance from the Google Cloud Platform Go
	// library.
	Logger GoogleCloudLogger

	// subLoggers is a map of logIDs to logging.Logger instances from the
	// Google Cloud Platform Go library, used when there are named zap
	// loggers based off the original.
	subLoggers map[string]GoogleCloudLogger

	// Used when accessing subLoggers
	loggerLock sync.Mutex

	// Provide your own mapping of zapcore's Levels to Google's Severities, or
	// use DefaultSeverityMapping. All of the Core's children will default to
	// using this map.
	//
	// This must not be mutated after the Core's first use.
	SeverityMapping map[zapcore.Level]gcl.Severity

	// MinLevel is the minimum level for a log entry to be written.
	MinLevel zapcore.Level

	// fields should be built once and never mutated again.
	fields map[string]interface{}

	// The Google logging client object.
	client *gcl.Client

	// The base of the logID (the last component of the logName, as
	// described in the GCP LogEntry documentation).
	baseLogID string
}

// Tee returns a zapcore.Core that writes entries to both the provided core
// and to Stackdriver using the provided client.  The provided gclLogID will
// form the base of each GCP LogEntry's logID, to which will be appended the zap
// logger name.
//
// For fields to be written to Stackdriver, you must use the With() method on
// the returned Core rather than just on zc. (This function has no way of
// knowing about fields that already exist on zc. They will be preserved when
// writing to zc's existing destination, but not to Stackdriver.)
func Tee(zc zapcore.Core, client *gcl.Client, gclLogID string) zapcore.Core {
	gc := &Core{
		Logger:          client.Logger(gclLogID),
		SeverityMapping: DefaultSeverityMapping,
		client:          client,
		baseLogID:       gclLogID,
	}

	for l := zapcore.DebugLevel; l <= zapcore.FatalLevel; l++ {
		if zc.Enabled(l) {
			gc.MinLevel = l
			break
		}
	}

	return zapcore.NewTee(zc, gc)
}

// Enabled implements zapcore.Core.
func (c *Core) Enabled(l zapcore.Level) bool {
	return l >= c.MinLevel
}

// With implements zapcore.Core.
func (c *Core) With(newFields []zapcore.Field) zapcore.Core {
	return &Core{
		Logger:          c.Logger,
		SeverityMapping: c.SeverityMapping,
		MinLevel:        c.MinLevel,
		fields:          clone(c.fields, newFields),
		client:          c.client,
		baseLogID:       c.baseLogID,
		subLoggers:      c.subLoggers,
	}
}

// Check implements zapcore.Core.
func (c *Core) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(e.Level) {
		return ce.AddCore(e, c)
	}
	return ce
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// Write implements zapcore.Core. It writes a log entry to Stackdriver.
//
// Certain fields in the zapcore.Entry are used to populate the Stackdriver
// entry: the Message field maps to "message" in the payload and the Stack field
// maps to "stack".  LoggerName helps populate the logName field in the log
// entry, as well as mapping to "logger" in the payload.
//
// If there is a field with key CallerKey and type CallerType, and it is a
// pkg/errors.stackTracer, then the first frame of the stack is put into the
// Stackdriver entry object's SourceLocation field, and the original field is
// removed from the payload sent to Stackdriver.  If not, then the Caller field
// from the zap Entry is used instead.
//
// If there is a field with key SkipKey (and any value, but type
// zapcore.SkipType would be the most useful), the core will skip writing this
// entry to Stackdriver.  This allows the application to prevent writing
// sensitive data to the cloud, while still allowing cores writing to the
// terminal to write those entries.
func (c *Core) Write(ze zapcore.Entry, newFields []zapcore.Field) error {
	severity, specified := c.SeverityMapping[ze.Level]
	if !specified {
		severity = gcl.Default
	}

	payload := clone(c.fields, newFields)

	// If the logger asked us to skip this entry, just return.
	if _, ok := payload[SkipKey]; ok {
		return nil
	}

	// We're putting LoggerName into the logName, so we're only keeping it
	// in the payload for expectations from older clients.
	if ze.LoggerName != "" {
		payload["logger"] = ze.LoggerName
	}
	if ze.Stack != "" {
		payload["stack"] = ze.Stack
	}
	payload["message"] = ze.Message

	entry := gcl.Entry{
		Timestamp: ze.Time,
		Severity:  severity,
		Payload:   payload,
	}

	insertID, ok := payload[InsertIDKey].(string)
	if ok && insertID != "" {
		entry.InsertID = insertID
	}
	delete(payload, InsertIDKey)

	if caller, ok := payload[CallerKey]; ok {
		if tracer, ok := caller.(stackTracer); ok {
			frame := tracer.StackTrace()[0]
			pc := uintptr(frame) - 1
			fn := runtime.FuncForPC(pc)
			file, line := fn.FileLine(pc)
			entry.SourceLocation = &logpb.LogEntrySourceLocation{
				File:     file,
				Line:     int64(line),
				Function: fn.Name(),
			}
			delete(payload, CallerKey)
		}
	} else if ze.Caller.Defined {
		entry.SourceLocation = &logpb.LogEntrySourceLocation{
			File:     ze.Caller.File,
			Line:     int64(ze.Caller.Line),
			Function: runtime.FuncForPC(ze.Caller.PC).Name(),
		}
	}

	name := c.baseLogID
	if ze.LoggerName != "" && name != "" {
		name += "." + ze.LoggerName
	} else if ze.LoggerName != "" {
		name = ze.LoggerName
	}

	// Find the logger given by name; if that's not available, then use the
	// default logger.
	c.loggerLock.Lock()
	logger, ok := c.subLoggers[name]
	if !ok {
		// The tests rely on the ability to have a nil client, since
		// they don't actually test logging to Stackdriver.  In that
		// case, fall back to the default logger.
		if c.client != nil {
			logger = c.client.Logger(name)
			if c.subLoggers == nil {
				c.subLoggers = make(map[string]GoogleCloudLogger)
			}
			c.subLoggers[name] = logger
		} else {
			logger = c.Logger
		}
	}
	c.loggerLock.Unlock()

	logger.Log(entry)

	return nil
}

// Sync implements zapcore.Core. It flushes the Core's Logger instance.
func (c *Core) Sync() error {
	var ret error

	if ret = c.Logger.Flush(); ret != nil {
		ret = newError("flushing Google Cloud logger: %v", ret)
	}

	c.loggerLock.Lock()
	defer c.loggerLock.Unlock()

	for _, logger := range c.subLoggers {
		if err := logger.Flush(); err != nil && ret == nil {
			ret = newError("flushing Google Cloud logger: %v", err)
		}
	}
	return ret
}

// DefaultSeverityMapping is the default mapping of zap's Levels to Google's
// Severities.
var DefaultSeverityMapping = map[zapcore.Level]gcl.Severity{
	zapcore.DebugLevel:  gcl.Debug,
	zapcore.InfoLevel:   gcl.Info,
	zapcore.WarnLevel:   gcl.Warning,
	zapcore.ErrorLevel:  gcl.Error,
	zapcore.DPanicLevel: gcl.Critical,
	zapcore.PanicLevel:  gcl.Critical,
	zapcore.FatalLevel:  gcl.Critical,
}

// clone creates a new field map without mutating the original.
func clone(orig map[string]interface{}, newFields []zapcore.Field) map[string]interface{} {
	clone := make(map[string]interface{})

	for k, v := range orig {
		clone[k] = v
	}

	for _, f := range newFields {
		switch f.Type {
		// case zapcore.UnknownType:
		case zapcore.ArrayMarshalerType:
			clone[f.Key] = f.Interface
		case zapcore.ObjectMarshalerType:
			clone[f.Key] = f.Interface
		case zapcore.BinaryType:
			clone[f.Key] = f.Interface
		case zapcore.BoolType:
			clone[f.Key] = (f.Integer == 1)
		case zapcore.ByteStringType:
			clone[f.Key] = f.String
		case zapcore.Complex128Type:
			clone[f.Key] = fmt.Sprint(f.Interface)
		case zapcore.Complex64Type:
			clone[f.Key] = fmt.Sprint(f.Interface)
		case zapcore.DurationType:
			clone[f.Key] = time.Duration(f.Integer).String()
		case zapcore.Float64Type:
			clone[f.Key] = float64(f.Integer)
		case zapcore.Float32Type:
			clone[f.Key] = float32(f.Integer)
		case zapcore.Int64Type:
			clone[f.Key] = int64(f.Integer)
		case zapcore.Int32Type:
			clone[f.Key] = int32(f.Integer)
		case zapcore.Int16Type:
			clone[f.Key] = int16(f.Integer)
		case zapcore.Int8Type:
			clone[f.Key] = int8(f.Integer)
		case zapcore.StringType:
			clone[f.Key] = f.String
		case zapcore.TimeType:
			// Handle uber-go/zap#425
			if f.Interface == nil {
				clone[f.Key] = time.Unix(0, f.Integer)
			} else {
				clone[f.Key] = time.Unix(0, f.Integer).In(f.Interface.(*time.Location))
			}
		case zapcore.Uint64Type:
			clone[f.Key] = uint64(f.Integer)
		case zapcore.Uint32Type:
			clone[f.Key] = uint32(f.Integer)
		case zapcore.Uint16Type:
			clone[f.Key] = uint16(f.Integer)
		case zapcore.Uint8Type:
			clone[f.Key] = uint8(f.Integer)
		case zapcore.UintptrType:
			clone[f.Key] = uintptr(f.Integer)
		case zapcore.ReflectType:
			clone[f.Key] = f.Interface
		// case zapcore.NamespaceType:
		case zapcore.StringerType:
			clone[f.Key] = f.Interface.(fmt.Stringer).String()
		case zapcore.ErrorType:
			clone[f.Key] = f.Interface.(error).Error()
		case zapcore.SkipType:
			// If we're hiding caller information here, then don't
			// actually skip.
			if f.Key == CallerKey || f.Key == SkipKey {
				clone[f.Key] = f.Interface
			}
		default:
			clone[f.Key] = f.Interface
		}
	}

	return clone
}

const packageName = "gcloudzap"

// newError calls fmt.Errorf() and prefixes the error with the packageName.
func newError(format string, args ...interface{}) error {
	return fmt.Errorf(packageName+": "+format, args)
}

// GoogleCloudLogger encapsulates the important methods of gcl.Logger
type GoogleCloudLogger interface {
	Flush() error
	Log(e gcl.Entry)
}
