package blob

import (
	"fmt"
	"testing"

	"github.com/project-flogo/core/activity"
	"github.com/project-flogo/core/support/test"
	"github.com/stretchr/testify/assert"
)

//todo add asserts

func TestRegister(t *testing.T) {

	ref := activity.GetRef(&Activity{})
	act := activity.Get(ref)

	assert.NotNil(t, act)
}

func TestSettings(t *testing.T) {
	settings := &Settings{Method: "upload",
		AZURE_STORAGE_ACCESS_KEY: "4KmJJLSbF2PUrWj08M4iWNxS68zDkkhUdBGBtUETYgB9KjLXn19HLdsGjonW6CBtlBJtS7VdPAjtwyL1rFEorA==", AZURE_STORAGE_ACCOUNT: "blobtesttibco",
		ContainerName: "sample"}

	iCtx := test.NewActivityInitContext(settings, nil)
	_, err := New(iCtx)
	assert.Nil(t, err)

}
func TestSimpleUpdate(t *testing.T) {
	settings := &Settings{Method: "upload",
		AZURE_STORAGE_ACCESS_KEY: "VU4lKfOQMV8YlzKj3GYJzqvntZEfBdAlb1qGaHRvCfW8Jmvc5k+27EWJa0vwapa3KjhivfN3BhulBm4auHlTrg==", AZURE_STORAGE_ACCOUNT: "blobtesttibco",
		ContainerName: "sample"}
	input := &Input{File: "sample.txt", Data: "Sample Blob"}

	iCtx := test.NewActivityInitContext(settings, nil)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())
	tc.SetInputObject(input)
	out, err := act.Eval(tc)

	assert.Nil(t, err)

	assert.True(t, out)

}

func TestSimpleList(t *testing.T) {
	settings := &Settings{Method: "list",
		AZURE_STORAGE_ACCESS_KEY: "VU4lKfOQMV8YlzKj3GYJzqvntZEfBdAlb1qGaHRvCfW8Jmvc5k+27EWJa0vwapa3KjhivfN3BhulBm4auHlTrg==", AZURE_STORAGE_ACCOUNT: "blobtesttibco",
		ContainerName: "sample"}

	iCtx := test.NewActivityInitContext(settings, nil)
	act, err := New(iCtx)
	assert.Nil(t, err)

	tc := test.NewActivityContext(act.Metadata())

	out, err := act.Eval(tc)
	output := &Output{}
	tc.GetOutputObject(output)
	fmt.Println("Output object", output)
	assert.Nil(t, err)

	assert.True(t, out)

}
