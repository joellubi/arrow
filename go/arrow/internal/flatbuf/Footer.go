// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by the FlatBuffers compiler. DO NOT EDIT.

package flatbuf

import (
	flatbuffers "github.com/google/flatbuffers/go"
)

// / ----------------------------------------------------------------------
// / Arrow File metadata
// /
type Footer struct {
	_tab flatbuffers.Table
}

func GetRootAsFooter(buf []byte, offset flatbuffers.UOffsetT) *Footer {
	n := flatbuffers.GetUOffsetT(buf[offset:])
	x := &Footer{}
	x.Init(buf, n+offset)
	return x
}

func (rcv *Footer) Init(buf []byte, i flatbuffers.UOffsetT) {
	rcv._tab.Bytes = buf
	rcv._tab.Pos = i
}

func (rcv *Footer) Table() flatbuffers.Table {
	return rcv._tab
}

func (rcv *Footer) Version() MetadataVersion {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(4))
	if o != 0 {
		return MetadataVersion(rcv._tab.GetInt16(o + rcv._tab.Pos))
	}
	return 0
}

func (rcv *Footer) MutateVersion(n MetadataVersion) bool {
	return rcv._tab.MutateInt16Slot(4, int16(n))
}

func (rcv *Footer) Schema(obj *Schema) *Schema {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(6))
	if o != 0 {
		x := rcv._tab.Indirect(o + rcv._tab.Pos)
		if obj == nil {
			obj = new(Schema)
		}
		obj.Init(rcv._tab.Bytes, x)
		return obj
	}
	return nil
}

func (rcv *Footer) Dictionaries(obj *Block, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 24
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Footer) DictionariesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(8))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

func (rcv *Footer) RecordBatches(obj *Block, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 24
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Footer) RecordBatchesLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(10))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

// / User-defined metadata
func (rcv *Footer) CustomMetadata(obj *KeyValue, j int) bool {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		x := rcv._tab.Vector(o)
		x += flatbuffers.UOffsetT(j) * 4
		x = rcv._tab.Indirect(x)
		obj.Init(rcv._tab.Bytes, x)
		return true
	}
	return false
}

func (rcv *Footer) CustomMetadataLength() int {
	o := flatbuffers.UOffsetT(rcv._tab.Offset(12))
	if o != 0 {
		return rcv._tab.VectorLen(o)
	}
	return 0
}

// / User-defined metadata
func FooterStart(builder *flatbuffers.Builder) {
	builder.StartObject(5)
}
func FooterAddVersion(builder *flatbuffers.Builder, version MetadataVersion) {
	builder.PrependInt16Slot(0, int16(version), 0)
}
func FooterAddSchema(builder *flatbuffers.Builder, schema flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(1, flatbuffers.UOffsetT(schema), 0)
}
func FooterAddDictionaries(builder *flatbuffers.Builder, dictionaries flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(2, flatbuffers.UOffsetT(dictionaries), 0)
}
func FooterStartDictionariesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(24, numElems, 8)
}
func FooterAddRecordBatches(builder *flatbuffers.Builder, recordBatches flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(3, flatbuffers.UOffsetT(recordBatches), 0)
}
func FooterStartRecordBatchesVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(24, numElems, 8)
}
func FooterAddCustomMetadata(builder *flatbuffers.Builder, customMetadata flatbuffers.UOffsetT) {
	builder.PrependUOffsetTSlot(4, flatbuffers.UOffsetT(customMetadata), 0)
}
func FooterStartCustomMetadataVector(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT {
	return builder.StartVector(4, numElems, 4)
}
func FooterEnd(builder *flatbuffers.Builder) flatbuffers.UOffsetT {
	return builder.EndObject()
}
