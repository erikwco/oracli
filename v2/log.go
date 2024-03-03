package oracli

import (
    "fmt"
    "io"
    "log"
    "os"
)

type Loggable interface {
    Info(msg string)
    Infof(mg string, v ...interface{})
    
    Warn(msg string)
    Warnf(mg string, v ...interface{})
    
    Debug(msg string)
    Debugf(mg string, v ...interface{})
    
    Error(msg string)
    Errorf(mg string, v ...interface{})
    
    Err(err error, msg string)
    Errf(err error, mg string, v ...interface{})
    
    Log(msg string)
    Logf(mg string, v ...interface{})
    
    Panic(msg string)
    Panicf(mg string, v ...interface{})
}

type CustomLog struct {
    logger *log.Logger
}

func (c CustomLog) Info(msg string) {
    c.logger.SetPrefix("INFO:")
    _ = c.logger.Output(2, msg)
}

func (c CustomLog) Infof(msg string, v ...interface{}) {
    c.logger.SetPrefix("INFO:")
    _ = c.logger.Output(2, fmt.Sprintf(msg, v...))
}

func (c CustomLog) Warn(msg string) {
    c.logger.SetPrefix("WARN:")
    _ = c.logger.Output(2, msg)
}

func (c CustomLog) Warnf(msg string, v ...interface{}) {
    c.logger.SetPrefix("WARN:")
    _ = c.logger.Output(2, fmt.Sprintf(msg, v...))
}

func (c CustomLog) Debug(msg string) {
    c.logger.SetPrefix("DEBUG:")
    _ = c.logger.Output(2, msg)
}

func (c CustomLog) Debugf(msg string, v ...interface{}) {
    c.logger.SetPrefix("DEBUG:")
    _ = c.logger.Output(2, fmt.Sprintf(msg, v...))
}

func (c CustomLog) Error(msg string) {
    c.logger.SetPrefix("ERROR:")
    _ = c.logger.Output(2, msg)
}

func (c CustomLog) Errorf(msg string, v ...interface{}) {
    c.logger.SetPrefix("ERROR:")
    _ = c.logger.Output(2, fmt.Sprintf(msg, v...))
}

func (c CustomLog) Log(msg string) {
    c.logger.SetPrefix("LOG:")
    _ = c.logger.Output(2, msg)
}

func (c CustomLog) Logf(msg string, v ...interface{}) {
    c.logger.SetPrefix("LOG:")
    _ = c.logger.Output(2, fmt.Sprintf(msg, v...))
}

func (c CustomLog) Panic(msg string) {
    c.logger.SetPrefix("PANIC:")
    _ = c.logger.Output(2, msg)
}

func (c CustomLog) Panicf(msg string, v ...interface{}) {
    c.logger.SetPrefix("PANIC:")
    _ = c.logger.Output(2, fmt.Sprintf(msg, v...))
}

func (c CustomLog) Err(err error, msg string) {
    c.logger.SetPrefix(fmt.Sprintf("ERR[%v]:", err.Error()))
    _ = c.logger.Output(2, msg)
}

func (c CustomLog) Errf(err error, msg string, v ...interface{}) {
    c.logger.SetPrefix(fmt.Sprintf("ERR[%v]:", err.Error()))
    _ = c.logger.Output(2, fmt.Sprintf(msg, v...))
}

func newLogger() Loggable {
    return CustomLog{logger: log.New(os.Stdout, "oracli", log.LstdFlags)}
}

func setCustomLog(input io.Writer) Loggable {
    return CustomLog{logger: log.New(input, "oracli", log.LstdFlags)}
}
