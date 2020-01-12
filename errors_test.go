package que

import (
	"errors"
	"testing"
)

func TestMultiErr(t *testing.T) {
	errs := new(MultiErr)
	errs.Append(nil, nil)
	if !errs.Empty() {
		t.Error("want Empty() returns true but get false")
	}
	if errs.Err() != nil {
		t.Errorf("want Err() returns nil but get %T(%v)", errs.Err(), errs.Err())
	}
	if errs.Is(errors.New("a err")) {
		t.Errorf("want Is() returns false")
	}

	e1 := errors.New("err1")
	e2 := errors.New("err2")

	errs2 := new(MultiErr)
	errs2.Append(e2)

	errs.Append(e1)
	if errs.Empty() {
		t.Error("want Empty() returns false")
	}
	if errs.Err() != e1 {
		t.Errorf("want Err() returns %v but get %v", e1, errs.Err())
	}

	errs.Append(errs2)
	if !errs.Is(e1) {
		t.Errorf("want Is(%v) returns true", e1)
	}
	if !errs.Is(e2) {
		t.Errorf("want Is(%v) returns true", e2)
	}
	if errs.Is(errs2) {
		t.Errorf("want Is(%v) returns false", errs2)
	}
	wantErr := `err1; err2`
	actualErr := errs.Err().Error()
	if wantErr != actualErr {
		t.Errorf("want Err().Error() returns %s but get %s", wantErr, actualErr)
	}
}
