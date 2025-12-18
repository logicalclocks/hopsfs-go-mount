// Copyright (c) Microsoft. All rights reserved.
// Copyright (c) Hopsworks AB. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package logger

import (
	"fmt"
	"os"
	"runtime"

	nested "github.com/antonfisher/nested-logrus-formatter"
	logger "github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)

var ReportCaller = true

func init() {
	InitLogger("info", false, "")
}

func InitLogger(l string, reportCaller bool, lfile string) {
	ReportCaller = reportCaller
	lvl, err := logger.ParseLevel(l)
	if err != nil {
		logger.Errorf("Invlid log level %s ", l)
		lvl = logger.ErrorLevel
	}

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	// TODO log to file and log cutting

	//Json output
	//logger.SetFormatter(&logger.JSONFormatter{})

	//set custom formatter github.com/antonfisher/nested-logrus-formatter
	logger.SetFormatter(&nested.Formatter{
		HideKeys:       false,
		NoFieldsColors: true,
		FieldsOrder:    []string{"op", "path", "bytes", "total_bytes_read", "total_bytes_written"},
	})

	// Only log the warning severity or above.
	logger.SetLevel(lvl)

	// setup log cutting
	if lfile != "" {
		logger.SetOutput(&lumberjack.Logger{
			Filename:   lfile,
			MaxSize:    100, // megabytes
			MaxBackups: 10,
			MaxAge:     30, //days
		})
	} else {
		logger.SetOutput(os.Stdout)
	}
}

type Fields logger.Fields

func Trace(msg string, f Fields) {
	Logmessage(logger.TraceLevel, msg, f)
}

func Debug(msg string, f Fields) {
	Logmessage(logger.DebugLevel, msg, f)
}

func Info(msg string, f Fields) {
	Logmessage(logger.InfoLevel, msg, f)
}

func Warn(msg string, f Fields) {
	Logmessage(logger.WarnLevel, msg, f)
}

func Error(msg string, f Fields) {
	Logmessage(logger.ErrorLevel, msg, f)
}

func Fatal(msg string, f Fields) {
	Logmessage(logger.FatalLevel, msg, f)
}

func Panic(msg string, f Fields) {
	Logmessage(logger.PanicLevel, msg, f)
}

func Logmessage(lvl logger.Level, msg string, f Fields) {
	if ReportCaller {
		_, file, line, _ := runtime.Caller(2)
		if f == nil {
			f = Fields{}
		}
		f["line"] = fmt.Sprintf("%s:%d", file, line)
	}

	switch lvl {
	case logger.PanicLevel:
		logger.WithFields(logger.Fields(f)).Panic(msg)
	case logger.FatalLevel:
		logger.WithFields(logger.Fields(f)).Fatal(msg)
	case logger.ErrorLevel:
		logger.WithFields(logger.Fields(f)).Error(msg)
	case logger.WarnLevel:
		logger.WithFields(logger.Fields(f)).Warn(msg)
	case logger.InfoLevel:
		logger.WithFields(logger.Fields(f)).Info(msg)
	case logger.DebugLevel:
		logger.WithFields(logger.Fields(f)).Debug(msg)
	case logger.TraceLevel:
		logger.WithFields(logger.Fields(f)).Trace(msg)
	default:
		logger.WithFields(logger.Fields(f)).Info(msg)
	}
}
