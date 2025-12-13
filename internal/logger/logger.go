package logger

import (
	"os"
	"sync"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	sugar *zap.SugaredLogger
	once  sync.Once
)

// Init initializes the global logger
func Init() *zap.SugaredLogger {
	once.Do(func() {
		var logger *zap.Logger
		var err error

		env := os.Getenv("ENV")
		if env == "production" || env == "prod" {
			// Production: JSON format, info level
			config := zap.NewProductionConfig()
			config.EncoderConfig.TimeKey = "timestamp"
			config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
			logger, err = config.Build()
		} else {
			// Development: console format, debug level
			config := zap.NewDevelopmentConfig()
			config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
			logger, err = config.Build()
		}

		if err != nil {
			panic("failed to initialize logger: " + err.Error())
		}

		sugar = logger.Sugar()
	})

	return sugar
}

// Get returns the global sugared logger (must call Init first)
func Get() *zap.SugaredLogger {
	if sugar == nil {
		return Init()
	}
	return sugar
}

// Sync flushes any buffered log entries
func Sync() {
	if sugar != nil {
		_ = sugar.Sync()
	}
}

// With creates a child logger with additional fields
func With(args ...any) *zap.SugaredLogger {
	return Get().With(args...)
}

// Debug logs a debug message
func Debug(args ...any) {
	Get().Debug(args...)
}

// Debugf logs a formatted debug message
func Debugf(template string, args ...any) {
	Get().Debugf(template, args...)
}

// Debugw logs a debug message with key-value pairs
func Debugw(msg string, keysAndValues ...any) {
	Get().Debugw(msg, keysAndValues...)
}

// Info logs an info message
func Info(args ...any) {
	Get().Info(args...)
}

// Infof logs a formatted info message
func Infof(template string, args ...any) {
	Get().Infof(template, args...)
}

// Infow logs an info message with key-value pairs
func Infow(msg string, keysAndValues ...any) {
	Get().Infow(msg, keysAndValues...)
}

// Warn logs a warning message
func Warn(args ...any) {
	Get().Warn(args...)
}

// Warnf logs a formatted warning message
func Warnf(template string, args ...any) {
	Get().Warnf(template, args...)
}

// Warnw logs a warning message with key-value pairs
func Warnw(msg string, keysAndValues ...any) {
	Get().Warnw(msg, keysAndValues...)
}

// Error logs an error message
func Error(args ...any) {
	Get().Error(args...)
}

// Errorf logs a formatted error message
func Errorf(template string, args ...any) {
	Get().Errorf(template, args...)
}

// Errorw logs an error message with key-value pairs
func Errorw(msg string, keysAndValues ...any) {
	Get().Errorw(msg, keysAndValues...)
}

// Fatal logs a fatal message and exits
func Fatal(args ...any) {
	Get().Fatal(args...)
}

// Fatalf logs a formatted fatal message and exits
func Fatalf(template string, args ...any) {
	Get().Fatalf(template, args...)
}

// Fatalw logs a fatal message with key-value pairs and exits
func Fatalw(msg string, keysAndValues ...any) {
	Get().Fatalw(msg, keysAndValues...)
}
