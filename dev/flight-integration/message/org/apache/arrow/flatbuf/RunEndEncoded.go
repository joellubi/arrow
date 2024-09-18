// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// Contains two child arrays, run_ends and values.
/// The run_ends child array must be a 16/32/64-bit integer array
/// which encodes the indices at which the run with the value in 
/// each corresponding index in the values child array ends.
/// Like list/struct types, the value array can be of any type.
type RunEndEncoded struct {
	_tab flatbuffers.Table
}

func GetRootAsRunEndEncoded(buf []byte, offset flatbuffers.UOffsetT) *RunEndEncoded {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &RunEndEncoded{}
	x.Init(buf, n+offset)
	return x
}

func FinishRunEndEncodedBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsRunEndEncoded(buf []byte, offset flatbuffers.UOffsetT) *RunEndEncoded {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &RunEndEncoded{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedRunEndEncodedBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *RunEndEncoded) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *RunEndEncoded) Table() flatbuffers.Table {
	return rcv._tab
}

func RunEndEncodedStart(builder *flatbuffers.Builder) {
	builder.StartObject(0)
}
func RunEndEncodedEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
