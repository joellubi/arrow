// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

/// Logically the same as Utf8, but the internal representation uses a view
/// struct that contains the string length and either the string's entire data
/// inline (for small strings) or an inlined prefix, an index of another buffer,
/// and an offset pointing to a slice in that buffer (for non-small strings).
///
/// Since it uses a variable number of data buffers, each Field with this type
/// must have a corresponding entry in `variadicBufferCounts`.
type Utf8View struct {
	_tab flatbuffers.Table
}

func GetRootAsUtf8View(buf []byte, offset flatbuffers.UOffsetT) *Utf8View {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Utf8View{}
	x.Init(buf, n+offset)
	return x
}

func FinishUtf8ViewBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.Finish(offset)
}

func GetSizePrefixedRootAsUtf8View(buf []byte, offset flatbuffers.UOffsetT) *Utf8View {
	n := flatbuffers.GetUOffsetT(buf[offset+flatbuffers.SizeUint32:])
	x := &Utf8View{}
	x.Init(buf, n+offset+flatbuffers.SizeUint32)
	return x
}

func FinishSizePrefixedUtf8ViewBuffer(builder *flatbuffers.Builder, offset flatbuffers.UOffsetT) {
	builder.FinishSizePrefixed(offset)
}

func (rcv *Utf8View) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Utf8View) Table() flatbuffers.Table {
	return rcv._tab
}

func Utf8ViewStart(builder *flatbuffers.Builder) {
	builder.StartObject(0)
}
func Utf8ViewEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
