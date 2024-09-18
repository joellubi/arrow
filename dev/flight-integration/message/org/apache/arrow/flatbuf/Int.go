// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Int struct {
	_tab flatbuffers.Table
}

func GetRootAsInt(buf []byte, offset flatbuffers.UOffsetT) *Int {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Int{}
	x.Init(buf, n+offset)
	return x
}

func FinishIntBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsInt(buf []byte, offset flatbuffers.UOffsetT) *Int {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Int{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedIntBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Int) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Int) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Int) BitWidth() int32 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt32(o + rcv._tab.Pos)
	}
	return 0
}

func (rcv *Int) MutateBitWidth(n int32) bool {
	return rcv._tab.MutateInt32Slot(4, n)
}

func (rcv *Int) IsSigned() bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		return rcv._tab.GetBool(o + rcv._tab.Pos)
	}
	return false
}

func (rcv *Int) MutateIsSigned(n bool) bool {
	return rcv._tab.MutateBoolSlot(6, n)
}

func IntStart(builder *flatbuffers.Builder) {
	builder.StartObject(2)
}
func IntAddBitWidth(builder *flatbuffers.Builder, bitWidth int32) {
	builder.PrependInt32Slot(0, bitWidth, 0)
}
func IntAddIsSigned(builder *flatbuffers.Builder, isSigned bool) {
	builder.PrependBoolSlot(1, isSigned, false)
}
func IntEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
