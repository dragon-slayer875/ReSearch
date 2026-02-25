package utils

import (
	"reflect"

	"github.com/go-playground/validator/v10"
)

type StructValidator struct {
	validate *validator.Validate
}

func New(validate *validator.Validate) *StructValidator {
	return &StructValidator{
		validate,
	}
}

func (v *StructValidator) Validate(out any) error {
	t := reflect.TypeOf(out)
	if t == nil {
		return nil
	}

	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}

	// only stucts for the validator
	if t.Kind() != reflect.Struct {
		return nil
	}

	return v.validate.Struct(out)
}
