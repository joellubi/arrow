// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

type DictionaryEncoding struct {
	_tab flatbuffers.Table
}

func GetRootAsDictionaryEncoding(buf []byte, offset flatbuffers.UOffsetT) *DictionaryEncoding {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &DictionaryEncoding{}
	x.Init(buf, n+offset)
	return x
}

func FinishDictionaryEncodingBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsDictionaryEncoding(buf []byte, offset flatbuffers.UOffsetT) *DictionaryEncoding {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &DictionaryEncoding{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedDictionaryEncodingBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *DictionaryEncoding) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *DictionaryEncoding) Table() flatbuffers.Table {
	return rcv._tab
}

/// The known dictionary id in the application where this data is used. In
/// the file or streaming formats, the dictionary ids are found in the
/// DictionaryBatch messages
func (rcv *DictionaryEncoding) Id() int64 {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return rcv._tab.GetInt64(o + rcv._tab.Pos)
	}
	return 0
}

/// The known dictionary id in the application where this data is used. In
/// the file or streaming formats, the dictionary ids are found in the
/// DictionaryBatch messages
func (rcv *DictionaryEncoding) MutateId(n int64) bool {
	return rcv._tab.MutateInt64Slot(4, n)
}

/// The dictionary indices are constrained to be non-negative integers. If
/// this field is null, the indices must be signed int32. To maximize
/// cross-language compatibility and performance, implementations are
/// recommended to prefer signed integer types over unsigned integer types
/// and to avoid uint64 indices unless they are required by an application.
func (rcv *DictionaryEncoding) IndexType(obj *Int) *Int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Int)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

/// The dictionary indices are constrained to be non-negative integers. If
/// this field is null, the indices must be signed int32. To maximize
/// cross-language compatibility and performance, implementations are
/// recommended to prefer signed integer types over unsigned integer types
/// and to avoid uint64 indices unless they are required by an application.
/// By default, dictionaries are not ordered, or the order does not have
/// semantic meaning. In some statistical, applications, dictionary-encoding
/// is used to represent ordered categorical data, and we provide a way to
/// preserve that metadata here
func (rcv *DictionaryEncoding) IsOrdered() bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.GetBool(o + rcv._tab.Pos)
	}
	return false
}

/// By default, dictionaries are not ordered, or the order does not have
/// semantic meaning. In some statistical, applications, dictionary-encoding
/// is used to represent ordered categorical data, and we provide a way to
/// preserve that metadata here
func (rcv *DictionaryEncoding) MutateIsOrdered(n bool) bool {
	return rcv._tab.MutateBoolSlot(8, n)
}

func (rcv *DictionaryEncoding) DictionaryKind() DictionaryKind {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return DictionaryKind(rcv._tab.GetInt16(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *DictionaryEncoding) MutateDictionaryKind(n DictionaryKind) bool {
	return rcv._tab.MutateInt16Slot(10, int16(n))
}

func DictionaryEncodingStart(builder *flatbuffers.Builder) {
	builder.StartObject(4)
}
func DictionaryEncodingAddId(builder *flatbuffers.Builder, id int64) {
	builder.PrependInt64Slot(0, id, 0)
}
func DictionaryEncodingAddIndexType(builder *flatbuffers.Builder, indexType flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(indexType), 0)
}
func DictionaryEncodingAddIsOrdered(builder *flatbuffers.Builder, isOrdered bool) {
	builder.PrependBoolSlot(2, isOrdered, false)
}
func DictionaryEncodingAddDictionaryKind(builder *flatbuffers.Builder, dictionaryKind DictionaryKind) {
	builder.PrependInt16Slot(3, int16(dictionaryKind), 0)
}
func DictionaryEncodingEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
