package util

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
)

/*
Wrap 添加堆栈信息
不需要每层都打印error日志，只要保证使用 Wrap 包装error，然后在最上层打印一遍即可

if err != nil {
    return errpkg.Wrap(err)
}
*/
func Wrap(err error) error {
	return WrapSkip(err, 1)
}

/*
WrapSkip 添加堆栈信息，skip代表跳过栈帧，多用于自定义error

自定义error实例：
type BizErr struct {
	ErrNo  int
	ErrMsg string
}

func NewBizErr(errNo int, errMsg string) error {
	bizErr := &BizErr{
		ErrNo:  errNo,
		ErrMsg: errMsg,
	}
	return errpkg.WrapSkip(bizErr, 1)
}

func (b *BizErr) Error() string {
	return fmt.Sprintf("errNo=[%d] errMsg=[%s]", b.ErrNo, b.ErrMsg)
}
*/
func WrapSkip(err error, skip int) error {
	if err == nil {
		return nil
	}
	pc, file, line, _ := runtime.Caller(1 + skip)
	fn := runtime.FuncForPC(pc)
	return fmt.Errorf("%w\n\tat %s ( %s:%d )", err, fn.Name(), filepath.Base(file), line)
}

// New return new error
func New(text string) error {
	err := errors.New(text)
	return WrapSkip(err, 1)
}

// Cause 返回最原始的错误，可用来断言
func Cause(err error) error {
	e := errors.Unwrap(err)
	if e == nil {
		return err
	}
	return Cause(e)
}
