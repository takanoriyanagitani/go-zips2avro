package util

import (
	"context"
)

type Io[T any] func(context.Context) (T, error)

type Void struct{}

var Empty Void = struct{}{}

func Bind[T, U any](
	i Io[T],
	f func(T) Io[U],
) Io[U] {
	return func(ctx context.Context) (u U, e error) {
		t, e := i(ctx)
		if nil != e {
			return u, e
		}
		return f(t)(ctx)
	}
}
