// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type Tensor struct {
	_tab flatbuffers.Table
}

func GetRootAsTensor(buf []byte, offset flatbuffers.UOffsetT) *Tensor {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Tensor{}
	x.Init(buf, n+offset)
	return x
}

func FinishTensorBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsTensor(buf []byte, offset flatbuffers.UOffsetT) *Tensor {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Tensor{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedTensorBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Tensor) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Tensor) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Tensor) TypeType() Type {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return Type(rcv._tab.GetByte(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *Tensor) MutateTypeType(n Type) bool {
	return rcv._tab.MutateByteSlot(4, byte(n))
}

/// The type of data contained in a value cell. Currently only fixed-width
/// value types are supported, no strings or nested types
func (rcv *Tensor) Type(obj *flatbuffers.Table) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		rcv._tab.Union(obj, o)
		return true
	}
	return false
}

/// The type of data contained in a value cell. Currently only fixed-width
/// value types are supported, no strings or nested types
/// The dimensions of the tensor, optionally named
func (rcv *Tensor) Shape(obj *TensorDim, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Tensor) ShapeLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// The dimensions of the tensor, optionally named
/// Non-negative byte offsets to advance one value cell along each dimension
/// If omitted, default to row-major order (C-like).
func (rcv *Tensor) Strides(j int) int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.GetInt64(a + flatbuffers.UOffsetT(j*8))
	}
	return 0
}

func (rcv *Tensor) StridesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

/// Non-negative byte offsets to advance one value cell along each dimension
/// If omitted, default to row-major order (C-like).
func (rcv *Tensor) MutateStrides(j int, n int64) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		a := rcv._tab.Vector(o)
		return rcv._tab.MutateInt64(a+flatbuffers.UOffsetT(j*8), n)
	}
	return false
}

/// The location and size of the tensor's data
func (rcv *Tensor) Data(obj *Buffer) *Buffer {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := o + rcv._tab.Pos
		if obj == nil {
			obj = new(Buffer)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

/// The location and size of the tensor's data
func TensorStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func TensorAddTypeType(builder *flatbuffers.Builder, typeType Type) {
	builder.PrependByteSlot(0, byte(typeType), 0)
}
func TensorAddType(builder *flatbuffers.Builder, type_ flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(type_), 0)
}
func TensorAddShape(builder *flatbuffers.Builder, shape flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(shape), 0)
}
func TensorStartShapeVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func TensorAddStrides(builder *flatbuffers.Builder, strides flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(strides), 0)
}
func TensorStartStridesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(8, numElems, 8)
}
func TensorAddData(builder *flatbuffers.Builder, data flatbuffers.UOffsetT) {
	builder.PrependStructSlot(4, flatbuffers.UOffsetT(data), 0)
}
func TensorEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
