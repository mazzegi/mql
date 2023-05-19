package mql

import (
	"fmt"
	"reflect"
	"testing"

	"golang.org/x/exp/constraints"
)

func AssertNoErr(t *testing.T, err error, format string, args ...any) {
	if err == nil {
		return
	}
	msg := fmt.Sprintf(format, args...)
	t.Fatalf("%s: error is not-nil but: %v", msg, err)
}

func AssertEqual(t *testing.T, want, have any) {
	if reflect.DeepEqual(want, have) {
		return
	}
	t.Fatalf("want %v, have %v", want, have)
}

func AssertInRange[T constraints.Ordered](t *testing.T, val, lower, upper T) {
	if val >= lower && val <= upper {
		return
	}
	t.Fatalf("%v not in range [%v, %v]", val, lower, upper)
}
