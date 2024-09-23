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

package cases

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/apache/arrow/dev/flight-integration/flight"
	"github.com/apache/arrow/dev/flight-integration/message/org/apache/arrow/flatbuf"
	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/tester"
	flatbuffers "github.com/google/flatbuffers/go"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

func init() {
	var (
		catalog               = "catalog"
		dbSchemaFilterPattern = "db_schema_filter_pattern"
		tableFilterPattern    = "table_filter_pattern"
		table                 = "table"
		dbSchema              = "db_schema"
		tableTypes            = []string{"table", "view"}
		pkCatalog             = "pk_catalog"
		pkDbSchema            = "pk_db_schema"
		pkTable               = "pk_table"
		fkCatalog             = "fk_catalog"
		fkDbSchema            = "fk_db_schema"
		fkTable               = "fk_table"

		stmtQuery       = "SELECT STATEMENT"
		stmtQueryHandle = "SELECT STATEMENT HANDLE"
		stmtUpdate      = "UPDATE STATEMENT"
		stmtUpdateRows  = int64(10000)

		stmtPreparedQuery        = "SELECT PREPARED STATEMENT"
		stmtPreparedQueryHandle  = "SELECT PREPARED STATEMENT HANDLE"
		stmtPreparedUpdate       = "UPDATE PREPARED STATEMENT"
		stmtPreparedUpdateHandle = "UPDATE PREPARED STATEMENT HANDLE"
		stmtPreparedUpdateRows   = int64(20000)

		createPreparedStatementActionType = "CreatePreparedStatement"
		closePreparedStatementActionType  = "ClosePreparedStatement"

		queryFields = []field{
			{
				Name:         "id",
				Type:         flatbuf.TypeInt,
				GetTypeTable: intTypeTable(64, true),
				Nullable:     true,
				Metadata: map[string]string{
					"ARROW:FLIGHT:SQL:TABLE_NAME":        "test",
					"ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT": "1",
					"ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE": "0",
					"ARROW:FLIGHT:SQL:TYPE_NAME":         "type_test",
					"ARROW:FLIGHT:SQL:SCHEMA_NAME":       "schema_test",
					"ARROW:FLIGHT:SQL:IS_SEARCHABLE":     "1",
					"ARROW:FLIGHT:SQL:CATALOG_NAME":      "catalog_test",
					"ARROW:FLIGHT:SQL:PRECISION":         "100",
				},
			},
		}
	)

	testcases := []struct {
		Command proto.Message
		Fields  []field
	}{
		{
			Command: &flight.CommandGetCatalogs{},
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetDbSchemas{Catalog: &catalog, DbSchemaFilterPattern: &dbSchemaFilterPattern},
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
			},
		},
		// {
		// 	Command: &flight.CommandGetTables{
		// 		Catalog:                &catalog,
		// 		DbSchemaFilterPattern:  &dbSchemaFilterPattern,
		// 		TableNameFilterPattern: &tableFilterPattern,
		// 		IncludeSchema:          false,
		// 		TableTypes:             tableTypes,
		// 	},
		// 	Fields: []field{
		// 		{Name: "catalog_name", Type: flatbuf.TypeUtf8, Nullable: true},
		// 		{Name: "db_schema_name", Type: flatbuf.TypeUtf8, Nullable: true},
		// 		{Name: "table_name", Type: flatbuf.TypeUtf8, Nullable: false},
		// 		{Name: "table_type", Type: flatbuf.TypeUtf8, Nullable: false},
		// 	},
		// },
		{
			Command: &flight.CommandGetTables{
				Catalog:                &catalog,
				DbSchemaFilterPattern:  &dbSchemaFilterPattern,
				TableNameFilterPattern: &tableFilterPattern,
				IncludeSchema:          true,
				TableTypes:             tableTypes,
			},
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "table_type", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "table_schema", Type: flatbuf.TypeBinary, GetTypeTable: binaryTypeTable(), Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetTableTypes{},
			Fields: []field{
				{Name: "table_type", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetPrimaryKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields: []field{
				{Name: "catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
			},
		},
		{
			Command: &flight.CommandGetExportedKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields: []field{
				{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(8, false), Nullable: false},
				{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(8, false), Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetImportedKeys{Catalog: &catalog, DbSchema: &dbSchema, Table: table},
			Fields: []field{
				{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(8, false), Nullable: false},
				{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(8, false), Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetCrossReference{
				PkCatalog:  &pkCatalog,
				PkDbSchema: &pkDbSchema,
				PkTable:    pkTable,
				FkCatalog:  &fkCatalog,
				FkDbSchema: &fkDbSchema,
				FkTable:    fkTable,
			},
			Fields: []field{
				{Name: "pk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "pk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "fk_catalog_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "fk_db_schema_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "fk_table_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "fk_column_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "key_sequence", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "fk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "pk_key_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "update_rule", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(8, false), Nullable: false},
				{Name: "delete_rule", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(8, false), Nullable: false},
			},
		},
		{
			Command: &flight.CommandGetXdbcTypeInfo{},
			Fields: []field{
				{Name: "type_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: false},
				{Name: "data_type", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "column_size", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: true},
				{Name: "literal_prefix", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "literal_suffix", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{
					Name:         "create_params",
					Type:         flatbuf.TypeList,
					GetTypeTable: listTypeTable(field{Name: "item", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable()}),
					Nullable:     true,
				},
				{Name: "nullable", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "case_sensitive", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable(), Nullable: false},
				{Name: "searchable", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "unsigned_attribute", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable(), Nullable: true},
				{Name: "fixed_prec_scale", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable(), Nullable: false},
				{Name: "auto_increment", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable(), Nullable: true},
				{Name: "local_type_name", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true},
				{Name: "minimum_scale", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: true},
				{Name: "maximum_scale", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: true},
				{Name: "sql_data_type", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: false},
				{Name: "datetime_subcode", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: true},
				{Name: "num_prec_radix", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: true},
				{Name: "interval_precision", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: true},
			},
		},
		{
			Command: &flight.CommandGetSqlInfo{Info: []uint32{0, 3}},
			Fields: []field{
				{Name: "info_name", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, false), Nullable: false},
				{
					Name: "value",
					Type: flatbuf.TypeUnion,
					GetTypeTable: unionTypeTable(
						flatbuf.UnionModeDense,
						[]field{
							{Name: "string_value", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable()},
							{Name: "bool_value", Type: flatbuf.TypeBool, GetTypeTable: boolTypeTable()},
							{Name: "bigint_value", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(64, true)},
							{Name: "int32_bitmask", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true)},
							{
								Name:         "string_list",
								Type:         flatbuf.TypeList,
								GetTypeTable: listTypeTable(field{Name: "item", Type: flatbuf.TypeUtf8, GetTypeTable: utf8TypeTable(), Nullable: true})},
							{
								Name: "int32_to_int32_list_map",
								Type: flatbuf.TypeMap,
								GetTypeTable: mapTypeTable(
									field{Name: "key", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true)},
									field{
										Name:         "val",
										Type:         flatbuf.TypeList,
										GetTypeTable: listTypeTable(field{Name: "item", Type: flatbuf.TypeInt, GetTypeTable: intTypeTable(32, true), Nullable: true}),
										Nullable:     true, // TODO: nullable?
									},
								),
							},
						},
					), Nullable: false},
			},
		},
	}

	steps := make([]scenario.ScenarioStep, 0)

	// ValidateMetadataRetrieval
	for _, tc := range testcases {
		name := proto.MessageName(tc.Command).Name()
		steps = append(
			steps,
			scenario.ScenarioStep{Name: fmt.Sprintf("GetFlightInfo/%s", name), ServerHandler: scenario.Handler{GetFlightInfo: echoFlightInfo}},
			scenario.ScenarioStep{Name: fmt.Sprintf("DoGet/%s", name), ServerHandler: scenario.Handler{DoGet: doGetFieldsForCommandFn(tc.Command, tc.Fields)}},
			scenario.ScenarioStep{Name: fmt.Sprintf("GetSchema/%s", name), ServerHandler: scenario.Handler{GetSchema: getSchemaFieldsForCommandFn(tc.Command, tc.Fields)}})
	}

	// ValidateStatementExecution
	steps = append(
		steps,
		scenario.ScenarioStep{
			Name: "GetFlightInfo/CommandStatementQuery",
			ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
				var cmd flight.CommandStatementQuery
				if err := deserializeProtobufWrappedInAny(fd.Cmd, &cmd); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if cmd.GetQuery() != stmtQuery {
					return nil, status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtQuery, cmd.GetQuery())
				}

				if len(cmd.GetTransactionId()) != 0 {
					return nil, status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				handle, err := createStatementQueryTicket([]byte(stmtQueryHandle))
				if err != nil {
					return nil, status.Errorf(codes.Internal, "failed to create Ticket: %s", err)
				}

				return &flight.FlightInfo{
					Endpoint: []*flight.FlightEndpoint{{
						Ticket: &flight.Ticket{Ticket: handle},
					}},
				}, nil
			}}},
		scenario.ScenarioStep{Name: "DoGet/TicketStatementQuery", ServerHandler: scenario.Handler{DoGet: func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
			var cmd flight.TicketStatementQuery
			if err := deserializeProtobufWrappedInAny(t.Ticket, &cmd); err != nil {
				return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
			}

			if string(cmd.GetStatementHandle()) != stmtQueryHandle {
				return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtQueryHandle, cmd.GetStatementHandle())
			}

			return fs.Send(&flight.FlightData{DataHeader: buildFlatbufferSchema(queryFields)})
		}}},
		scenario.ScenarioStep{Name: "GetSchema/CommandStatementQuery", ServerHandler: scenario.Handler{GetSchema: getSchemaFieldsForCommandFn(&flight.CommandStatementQuery{}, queryFields)}},

		scenario.ScenarioStep{
			Name: "DoPut/CommandStatementUpdate",
			ServerHandler: scenario.Handler{DoPut: func(fs flight.FlightService_DoPutServer) error {
				data, err := fs.Recv()
				if err != nil {
					return status.Errorf(codes.Internal, "unable to read from stream: %s", err)
				}

				desc := data.FlightDescriptor
				var cmd flight.CommandStatementUpdate
				if err := deserializeProtobufWrappedInAny(desc.Cmd, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if cmd.GetQuery() != stmtUpdate {
					return status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtUpdate, cmd.GetQuery())
				}

				if len(cmd.GetTransactionId()) != 0 {
					return status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				appMetadata, err := proto.Marshal(&flight.DoPutUpdateResult{RecordCount: stmtUpdateRows})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to marshal DoPutUpdateResult: %s", err)
				}

				return fs.Send(&flight.PutResult{AppMetadata: appMetadata})
			}}},
	)

	// ValidatePreparedStatementExecution
	steps = append(
		steps,
		scenario.ScenarioStep{
			Name: "DoAction/ActionCreatePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionCreatePreparedStatementRequest
				if err := deserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if req.GetQuery() != stmtPreparedQuery {
					return status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtPreparedQuery, req.GetQuery())
				}

				if len(req.GetTransactionId()) != 0 {
					return status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				body, err := serializeProtobufWrappedInAny(&flight.ActionCreatePreparedStatementResult{PreparedStatementHandle: []byte(stmtPreparedQueryHandle)})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to ActionCreatePreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.Result{Body: body})
			}}},
		scenario.ScenarioStep{
			Name: "DoPut/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{DoPut: func(fs flight.FlightService_DoPutServer) error {
				data, err := fs.Recv()
				if err != nil {
					return status.Errorf(codes.Internal, "unable to read from stream: %s", err)
				}

				msg := flatbuf.GetRootAsMessage(data.DataHeader, 0)
				if msg.HeaderType() != flatbuf.MessageHeaderSchema {
					return status.Errorf(codes.Internal, "invalid stream, expected first message to be Schema: %s", err)
				}

				fields, ok := parseFlatbufferSchemaFields(msg)
				if !ok {
					return status.Errorf(codes.Internal, "failed to parse flatbuffer schema")
				}

				// TODO: maybe don't use tester here
				t := tester.NewTester()
				assertSchemaMatchesFields(t, fields, queryFields)
				if len(t.Errors()) > 0 {
					return status.Errorf(codes.Internal, "flatbuffer schema mismatch: %s", errors.Join(t.Errors()...))
				}

				desc := data.FlightDescriptor
				var cmd flight.CommandPreparedStatementQuery
				if err := deserializeProtobufWrappedInAny(desc.Cmd, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				appMetadata, err := proto.Marshal(&flight.DoPutPreparedStatementResult{PreparedStatementHandle: cmd.GetPreparedStatementHandle()})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to marshal DoPutPreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.PutResult{AppMetadata: appMetadata})
			}}},
		scenario.ScenarioStep{
			Name: "GetFlightInfo/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{GetFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
				var cmd flight.CommandPreparedStatementQuery
				if err := deserializeProtobufWrappedInAny(fd.Cmd, &cmd); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return nil, status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				return &flight.FlightInfo{
					Endpoint: []*flight.FlightEndpoint{{
						Ticket: &flight.Ticket{Ticket: fd.Cmd},
					}},
				}, nil
			}}},
		scenario.ScenarioStep{
			Name: "DoGet/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{DoGet: func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
				var cmd flight.CommandPreparedStatementQuery
				if err := deserializeProtobufWrappedInAny(t.Ticket, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				return fs.Send(&flight.FlightData{DataHeader: buildFlatbufferSchema(queryFields)})
			}}},
		scenario.ScenarioStep{
			Name: "GetSchema/CommandPreparedStatementQuery",
			ServerHandler: scenario.Handler{GetSchema: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
				var cmd flight.CommandPreparedStatementQuery
				if err := deserializeProtobufWrappedInAny(fd.Cmd, &cmd); err != nil {
					return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return nil, status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, cmd.GetPreparedStatementHandle())
				}

				return &flight.SchemaResult{Schema: writeFlatbufferPayload(queryFields)}, nil
			}}},
		scenario.ScenarioStep{
			Name: "DoAction/ActionClosePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionClosePreparedStatementRequest
				if err := deserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if string(req.GetPreparedStatementHandle()) != stmtPreparedQueryHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedQueryHandle, req.GetPreparedStatementHandle())
				}

				return fs.Send(&flight.Result{})
			}}},

		scenario.ScenarioStep{
			Name: "DoAction/ActionCreatePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionCreatePreparedStatementRequest
				if err := deserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if req.GetQuery() != stmtPreparedUpdate {
					return status.Errorf(codes.InvalidArgument, "expected query: %s, found: %s", stmtPreparedUpdate, req.GetQuery())
				}

				if len(req.GetTransactionId()) != 0 {
					return status.Errorf(codes.InvalidArgument, "expected no TransactionID")
				}

				body, err := serializeProtobufWrappedInAny(&flight.ActionCreatePreparedStatementResult{PreparedStatementHandle: []byte(stmtPreparedUpdateHandle)})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to ActionCreatePreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.Result{Body: body})
			}}},
		scenario.ScenarioStep{
			Name: "DoPut/CommandPreparedStatementUpdate",
			ServerHandler: scenario.Handler{DoPut: func(fs flight.FlightService_DoPutServer) error {
				data, err := fs.Recv()
				if err != nil {
					return status.Errorf(codes.Internal, "unable to read from stream: %s", err)
				}

				msg := flatbuf.GetRootAsMessage(data.DataHeader, 0)
				if msg.HeaderType() != flatbuf.MessageHeaderSchema {
					return status.Errorf(codes.Internal, "invalid stream, expected first message to be Schema: %s", err)
				}

				fields, ok := parseFlatbufferSchemaFields(msg)
				if !ok {
					return status.Errorf(codes.Internal, "failed to parse flatbuffer schema")
				}

				if len(fields) != 0 {
					return status.Errorf(codes.InvalidArgument, "bind schema not expected")
				}

				desc := data.FlightDescriptor
				var cmd flight.CommandPreparedStatementUpdate
				if err := deserializeProtobufWrappedInAny(desc.Cmd, &cmd); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
				}

				if string(cmd.GetPreparedStatementHandle()) != stmtPreparedUpdateHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedUpdateHandle, cmd.GetPreparedStatementHandle())
				}

				appMetadata, err := proto.Marshal(&flight.DoPutUpdateResult{RecordCount: stmtPreparedUpdateRows})
				if err != nil {
					return status.Errorf(codes.Internal, "failed to marshal DoPutPreparedStatementResult: %s", err)
				}

				return fs.Send(&flight.PutResult{AppMetadata: appMetadata})
			}}},
		scenario.ScenarioStep{
			Name: "DoAction/ActionClosePreparedStatementRequest",
			ServerHandler: scenario.Handler{DoAction: func(a *flight.Action, fs flight.FlightService_DoActionServer) error {
				var req flight.ActionClosePreparedStatementRequest
				if err := deserializeProtobufWrappedInAny(a.Body, &req); err != nil {
					return status.Errorf(codes.InvalidArgument, "failed to deserialize Action.Body: %s", err)
				}

				if string(req.GetPreparedStatementHandle()) != stmtPreparedUpdateHandle {
					return status.Errorf(codes.InvalidArgument, "expected handle: %s, found: %s", stmtPreparedUpdateHandle, req.GetPreparedStatementHandle())
				}

				return fs.Send(&flight.Result{})
			}}},
	)

	scenario.Register(
		scenario.Scenario{
			Name:  "flight_sql",
			Steps: steps,
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {

				// ValidateMetadataRetrieval
				////////////////////////////
				for _, tc := range testcases {
					// pack the command
					desc, err := descForCommand(tc.Command)
					t.Require().NoError(err)

					// submit query
					info, err := client.GetFlightInfo(ctx, desc)
					t.Require().NoError(err)

					t.Require().Greater(len(info.Endpoint), 0)
					t.Assert().Equal(desc.Cmd, info.Endpoint[0].Ticket.Ticket)

					// fetch result stream
					stream, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
					t.Require().NoError(err)

					// validate first message is properly formatted schema message
					requireStreamHeaderMatchesFields(t, stream, tc.Fields)

					// drain rest of stream
					requireDrainStream(t, stream, func(t *tester.Tester, data *flight.FlightData) {
						// no more schema messages
						t.Assert().Contains(
							[]flatbuf.MessageHeader{
								flatbuf.MessageHeaderRecordBatch,
								flatbuf.MessageHeaderDictionaryBatch,
							},
							flatbuf.GetRootAsMessage(data.DataHeader, 0).HeaderType(),
						)
					})

					// issue GetSchema
					res, err := client.GetSchema(ctx, desc)
					t.Require().NoError(err)

					// expect schema to be serialized as full IPC stream Schema message
					requireSchemaResultMatchesFields(t, res, tc.Fields)
				}

				// ValidateStatementExecution
				/////////////////////////////
				{
					{
						desc, err := descForCommand(&flight.CommandStatementQuery{Query: stmtQuery})
						t.Require().NoError(err)

						info, err := client.GetFlightInfo(ctx, desc)
						t.Require().NoError(err)

						t.Require().Greater(len(info.Endpoint), 0)

						// fetch result stream
						stream, err := client.DoGet(ctx, info.Endpoint[0].Ticket)
						t.Require().NoError(err)

						// validate result stream
						requireStreamHeaderMatchesFields(t, stream, queryFields)
						requireDrainStream(t, stream, func(t *tester.Tester, data *flight.FlightData) {
							// no more schema messages
							t.Assert().Contains(
								[]flatbuf.MessageHeader{
									flatbuf.MessageHeaderRecordBatch,
									flatbuf.MessageHeaderDictionaryBatch,
								},
								flatbuf.GetRootAsMessage(data.DataHeader, 0).HeaderType(),
							)
						})

						res, err := client.GetSchema(ctx, desc)
						t.Require().NoError(err)

						// expect schema to be serialized as full IPC stream Schema message
						requireSchemaResultMatchesFields(t, res, queryFields)
					}

					{
						desc, err := descForCommand(&flight.CommandStatementUpdate{Query: stmtUpdate})
						t.Require().NoError(err)

						stream, err := client.DoPut(ctx)
						t.Require().NoError(err)

						t.Require().NoError(stream.Send(&flight.FlightData{FlightDescriptor: desc}))
						t.Require().NoError(stream.CloseSend())

						putResult, err := stream.Recv()
						t.Require().NoError(err)

						var updateResult flight.DoPutUpdateResult
						t.Require().NoError(proto.Unmarshal(putResult.GetAppMetadata(), &updateResult))

						t.Assert().Equal(stmtUpdateRows, updateResult.GetRecordCount())
					}
				}

				// ValidatePreparedStatementExecution
				/////////////////////////////////////
				{
					var prepareResult flight.ActionCreatePreparedStatementResult
					{
						prepareAction, err := packAction(
							createPreparedStatementActionType,
							&flight.ActionCreatePreparedStatementRequest{Query: stmtPreparedQuery},
							serializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, prepareAction)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())

						result, err := stream.Recv()
						t.Require().NoError(err)

						t.Require().NoError(deserializeProtobufWrappedInAny(result.Body, &prepareResult))

						t.Require().Equal(stmtPreparedQueryHandle, string(prepareResult.GetPreparedStatementHandle()))
						requireDrainStream(t, stream, nil)
					}

					var doPutPreparedResult flight.DoPutPreparedStatementResult
					{
						stream, err := client.DoPut(ctx)
						t.Require().NoError(err)

						desc, err := descForCommand(&flight.CommandPreparedStatementQuery{PreparedStatementHandle: prepareResult.GetPreparedStatementHandle()})
						t.Require().NoError(err)

						t.Require().NoError(stream.Send(&flight.FlightData{FlightDescriptor: desc, DataHeader: buildFlatbufferSchema(queryFields)}))
						t.Require().NoError(stream.CloseSend())

						putResult, err := stream.Recv()
						t.Require().NoError(err)

						// TODO: legacy server doesn't provide a response

						t.Require().NoError(proto.Unmarshal(putResult.GetAppMetadata(), &doPutPreparedResult))
						t.Require().Equal(stmtPreparedQueryHandle, string(doPutPreparedResult.GetPreparedStatementHandle()))
					}

					var (
						descPutPrepared *flight.FlightDescriptor
						ticket          *flight.Ticket
					)
					{
						desc, err := descForCommand(&flight.CommandPreparedStatementQuery{PreparedStatementHandle: doPutPreparedResult.GetPreparedStatementHandle()})
						t.Require().NoError(err)

						info, err := client.GetFlightInfo(ctx, desc)
						t.Require().NoError(err)

						t.Require().Greater(len(info.Endpoint), 0)

						descPutPrepared = desc
						ticket = info.Endpoint[0].Ticket
					}

					{
						stream, err := client.DoGet(ctx, ticket)
						t.Require().NoError(err)

						// validate result stream
						requireStreamHeaderMatchesFields(t, stream, queryFields)
						requireDrainStream(t, stream, func(t *tester.Tester, data *flight.FlightData) {
							// no more schema messages
							t.Assert().Contains(
								[]flatbuf.MessageHeader{
									flatbuf.MessageHeaderRecordBatch,
									flatbuf.MessageHeaderDictionaryBatch,
								},
								flatbuf.GetRootAsMessage(data.DataHeader, 0).HeaderType(),
							)
						})
					}

					{
						schema, err := client.GetSchema(ctx, descPutPrepared)
						t.Require().NoError(err)

						requireSchemaResultMatchesFields(t, schema, queryFields)
					}

					{
						action, err := packAction(
							closePreparedStatementActionType,
							&flight.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(stmtPreparedQueryHandle)},
							serializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, action)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())
						requireDrainStream(t, stream, nil)
					}

					{
						action, err := packAction(
							createPreparedStatementActionType,
							&flight.ActionCreatePreparedStatementRequest{Query: stmtPreparedUpdate},
							serializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, action)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())

						result, err := stream.Recv()
						t.Require().NoError(err)

						var actionResult flight.ActionCreatePreparedStatementResult
						t.Require().NoError(deserializeProtobufWrappedInAny(result.Body, &actionResult))

						t.Require().Equal(stmtPreparedUpdateHandle, string(actionResult.GetPreparedStatementHandle()))
						requireDrainStream(t, stream, nil)
					}

					{
						stream, err := client.DoPut(ctx)
						t.Require().NoError(err)

						desc, err := descForCommand(&flight.CommandPreparedStatementUpdate{PreparedStatementHandle: []byte(stmtPreparedUpdateHandle)})
						t.Require().NoError(err)

						t.Require().NoError(stream.Send(&flight.FlightData{FlightDescriptor: desc, DataHeader: buildFlatbufferSchema(nil)}))
						t.Require().NoError(stream.CloseSend())

						putResult, err := stream.Recv()
						t.Require().NoError(err)

						var updateResult flight.DoPutUpdateResult
						t.Require().NoError(proto.Unmarshal(putResult.GetAppMetadata(), &updateResult))

						t.Assert().Equal(stmtPreparedUpdateRows, updateResult.GetRecordCount())
					}

					{
						action, err := packAction(
							closePreparedStatementActionType,
							&flight.ActionClosePreparedStatementRequest{PreparedStatementHandle: []byte(stmtPreparedUpdateHandle)},
							serializeProtobufWrappedInAny,
						)
						t.Require().NoError(err)

						stream, err := client.DoAction(ctx, action)
						t.Require().NoError(err)

						t.Require().NoError(stream.CloseSend())
						requireDrainStream(t, stream, nil)
					}
				}
			},
		},
	)
}

type typeTableFn func(*flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT)

type field struct {
	Name         string
	Type         flatbuf.Type
	Nullable     bool
	Metadata     map[string]string
	GetTypeTable typeTableFn
}

func utf8TypeTable() typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.Utf8Start(b)
		return flatbuf.Utf8End(b), 0
	}
}

func binaryTypeTable() typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.BinaryStart(b)
		return flatbuf.BinaryEnd(b), 0
	}
}

func intTypeTable(bitWidth int32, isSigned bool) typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.IntStart(b)
		flatbuf.IntAddBitWidth(b, bitWidth)
		flatbuf.IntAddIsSigned(b, isSigned)
		return flatbuf.IntEnd(b), 0
	}
}

func boolTypeTable() typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		flatbuf.BoolStart(b)
		return flatbuf.BoolEnd(b), 0
	}
}

func listTypeTable(child field) typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		childOffset := buildFlatbufferField(b, child)

		flatbuf.ListStart(b)
		listOffset := flatbuf.ListEnd(b)

		flatbuf.FieldStartChildrenVector(b, 1)
		b.PrependUOffsetT(childOffset)
		childVecOffset := b.EndVector(1)

		return listOffset, childVecOffset
	}
}

func structTypeTable(children []field) typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		nChildren := len(children)
		childOffsets := make([]flatbuffers.UOffsetT, nChildren)
		for i, child := range children {
			childOffsets[i] = buildFlatbufferField(b, child)
		}

		flatbuf.Struct_Start(b)
		for i := nChildren - 1; i >= 0; i-- {
			b.PrependUOffsetT(childOffsets[i])
		}
		structOffset := flatbuf.Struct_End(b)

		flatbuf.FieldStartChildrenVector(b, nChildren)
		for i := nChildren - 1; i >= 0; i-- {
			b.PrependUOffsetT(childOffsets[i])
		}
		childVecOffset := b.EndVector(nChildren)

		return structOffset, childVecOffset
	}
}

func mapTypeTable(key, val field) typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		childOffset := buildFlatbufferField(
			b,
			field{
				Name:         "entries",
				Type:         flatbuf.TypeStruct_,
				GetTypeTable: structTypeTable([]field{key, val}),
			},
		)

		flatbuf.MapStart(b)
		mapOffset := flatbuf.MapEnd(b)

		flatbuf.FieldStartChildrenVector(b, 1)
		b.PrependUOffsetT(childOffset)
		childVecOffset := b.EndVector(1)

		return mapOffset, childVecOffset
	}
}

func unionTypeTable(mode flatbuf.UnionMode, children []field) typeTableFn {
	return func(b *flatbuffers.Builder) (flatbuffers.UOffsetT, flatbuffers.UOffsetT) {
		childOffsets := make([]flatbuffers.UOffsetT, len(children))
		for i, child := range children {
			childOffsets[i] = buildFlatbufferField(b, child)
		}

		flatbuf.UnionStartTypeIdsVector(b, len(children))
		for i := len(children) - 1; i >= 0; i-- {
			b.PlaceInt32(int32(i))
		}
		typeIDVecOffset := b.EndVector(len(children))

		flatbuf.UnionStart(b)
		flatbuf.UnionAddMode(b, mode)
		flatbuf.UnionAddTypeIds(b, typeIDVecOffset)
		unionOffset := flatbuf.UnionEnd(b)

		flatbuf.FieldStartChildrenVector(b, len(children))
		for i := len(children) - 1; i >= 0; i-- {
			b.PrependUOffsetT(childOffsets[i])
		}
		childVecOffset := b.EndVector(len(children))

		return unionOffset, childVecOffset
	}
}

func matchFieldByName(fields []flatbuf.Field, name string) (flatbuf.Field, bool) {
	for _, f := range fields {
		fieldName := string(f.Name())
		if fieldName == name {
			return f, true
		}
	}
	return flatbuf.Field{}, false
}

func writeFlatbufferPayload(fields []field) []byte {
	schema := buildFlatbufferSchema(fields)
	size := uint32(len(schema))

	res := make([]byte, 8+size)
	res[0] = 255
	res[1] = 255
	res[2] = 255
	res[3] = 255
	binary.LittleEndian.PutUint32(res[4:], size)
	copy(res[8:], schema)

	return res
}

func buildFlatbufferSchema(fields []field) []byte {
	b := flatbuffers.NewBuilder(1024)

	fieldOffsets := make([]flatbuffers.UOffsetT, len(fields))
	for i, f := range fields {
		fieldOffsets[len(fields)-i-1] = buildFlatbufferField(b, f)
	}

	flatbuf.SchemaStartFieldsVector(b, len(fields))

	for _, f := range fieldOffsets {
		b.PrependUOffsetT(f)
	}

	fieldsFB := b.EndVector(len(fields))

	flatbuf.SchemaStart(b)
	flatbuf.SchemaAddFields(b, fieldsFB)
	headerOffset := flatbuf.SchemaEnd(b)

	flatbuf.MessageStart(b)
	flatbuf.MessageAddVersion(b, flatbuf.MetadataVersionV5)
	flatbuf.MessageAddHeaderType(b, flatbuf.MessageHeaderSchema)
	flatbuf.MessageAddHeader(b, headerOffset)
	msg := flatbuf.MessageEnd(b)

	b.Finish(msg)

	return b.FinishedBytes()
}

func buildFlatbufferField(b *flatbuffers.Builder, f field) flatbuffers.UOffsetT {
	nameOffset := b.CreateString(f.Name)
	typOffset, childrenOffset := f.GetTypeTable(b)

	var kvOffsets []flatbuffers.UOffsetT
	for k, v := range f.Metadata {
		kk := b.CreateString(k)
		vv := b.CreateString(v)
		flatbuf.KeyValueStart(b)
		flatbuf.KeyValueAddKey(b, kk)
		flatbuf.KeyValueAddValue(b, vv)
		kvOffsets = append(kvOffsets, flatbuf.KeyValueEnd(b))
	}

	var metadataOffset flatbuffers.UOffsetT
	if len(kvOffsets) > 0 {
		flatbuf.FieldStartCustomMetadataVector(b, len(kvOffsets))
		for i := len(kvOffsets) - 1; i >= 0; i-- {
			b.PrependUOffsetT(kvOffsets[i])
		}
		metadataOffset = b.EndVector(len(kvOffsets))
	}

	flatbuf.FieldStart(b)
	flatbuf.FieldAddName(b, nameOffset)
	flatbuf.FieldAddTypeType(b, f.Type)
	flatbuf.FieldAddType(b, typOffset)
	flatbuf.FieldAddChildren(b, childrenOffset)
	flatbuf.FieldAddCustomMetadata(b, metadataOffset)
	flatbuf.FieldAddNullable(b, f.Nullable)
	return flatbuf.FieldEnd(b)
}

func parseFlatbufferSchemaFields(msg *flatbuf.Message) ([]flatbuf.Field, bool) {
	var schema flatbuf.Schema
	table := schema.Table()
	if ok := msg.Header(&table); !ok {
		return nil, false
	}
	schema.Init(table.Bytes, table.Pos)

	fields := make([]flatbuf.Field, schema.FieldsLength())
	for i := range fields {
		var field flatbuf.Field
		if ok := schema.Fields(&field, i); !ok {
			return nil, false
		}
		fields[i] = field
	}
	return fields, true
}

func extractFlatbufferPayload(b []byte) []byte {
	b = consumeContinuationIndicator(b)
	b, size := consumeMetadataSize(b)
	return b[:size]
}

func consumeMetadataSize(b []byte) ([]byte, int32) {
	size := int32(binary.LittleEndian.Uint32(b[:4]))
	return b[4:], size
}

func consumeContinuationIndicator(b []byte) []byte {
	indicator := []byte{255, 255, 255, 255}
	for i, v := range indicator {
		if b[i] != v {
			// indicator not found
			return b
		}
	}
	// indicator found, truncate leading bytes
	return b[4:]
}

func descForCommand(cmd proto.Message) (*flight.FlightDescriptor, error) {
	var any anypb.Any
	if err := any.MarshalFrom(cmd); err != nil {
		return nil, err
	}

	data, err := proto.Marshal(&any)
	if err != nil {
		return nil, err
	}
	return &flight.FlightDescriptor{
		Type: flight.FlightDescriptor_CMD,
		Cmd:  data,
	}, nil
}

func echoFlightInfo(ctx context.Context, fd *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	return &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: fd.Cmd},
		}},
	}, nil
}

func doGetFieldsForCommandFn(cmd proto.Message, fields []field) func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
	return func(t *flight.Ticket, fs flight.FlightService_DoGetServer) error {
		cmd := proto.Clone(cmd)
		proto.Reset(cmd)

		if err := deserializeProtobufWrappedInAny(t.Ticket, cmd); err != nil {
			return status.Errorf(codes.InvalidArgument, "failed to deserialize Ticket.Ticket: %s", err)
		}

		return fs.Send(&flight.FlightData{DataHeader: buildFlatbufferSchema(fields)})
	}
}

func getSchemaFieldsForCommandFn(cmd proto.Message, fields []field) func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	return func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.SchemaResult, error) {
		cmd := proto.Clone(cmd)
		proto.Reset(cmd)

		if err := deserializeProtobufWrappedInAny(fd.Cmd, cmd); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to deserialize FlightDescriptor.Cmd: %s", err)
		}

		schema := writeFlatbufferPayload(fields)

		return &flight.SchemaResult{Schema: schema}, nil
	}
}

func deserializeProtobufWrappedInAny(b []byte, dst proto.Message) error {
	var anycmd anypb.Any
	if err := proto.Unmarshal(b, &anycmd); err != nil {
		return fmt.Errorf("unable to unmarshal payload to proto.Any: %s", err)
	}

	if err := anycmd.UnmarshalTo(dst); err != nil {
		return fmt.Errorf("unable to unmarshal proto.Any: %s", err)
	}

	return nil
}

func deserializeProtobuf(b []byte, dst proto.Message) error {
	if err := proto.Unmarshal(b, dst); err != nil {
		return fmt.Errorf("unable to unmarshal protobuf payload: %s", err)
	}

	return nil
}

func createStatementQueryTicket(handle []byte) ([]byte, error) {
	query := &flight.TicketStatementQuery{StatementHandle: handle}

	var ticket anypb.Any
	if err := ticket.MarshalFrom(query); err != nil {
		return nil, fmt.Errorf("unable to marshal ticket proto to proto.Any: %s", err)
	}

	b, err := proto.Marshal(&ticket)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal proto.Any to bytes: %s", err)
	}

	return b, nil
}

func serializeProtobufWrappedInAny(msg proto.Message) ([]byte, error) {
	var anycmd anypb.Any
	if err := anycmd.MarshalFrom(msg); err != nil {
		return nil, fmt.Errorf("unable to marshal proto message to proto.Any: %s", err)
	}

	b, err := proto.Marshal(&anycmd)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal proto.Any to bytes: %s", err)
	}

	return b, nil
}

func serializeProtobuf(msg proto.Message) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal protobuf message to bytes: %s", err)
	}

	return b, nil
}

func packAction(actionType string, msg proto.Message, serialize func(proto.Message) ([]byte, error)) (*flight.Action, error) {
	var action flight.Action
	body, err := serialize(msg)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize action body: %s", err)
	}

	action.Type = actionType
	action.Body = body

	return &action, nil
}

func requireStreamHeaderMatchesFields[T interface {
	Recv() (*flight.FlightData, error)
}](t *tester.Tester, stream T, expectedFields []field) {

	data, err := stream.Recv()
	t.Require().NoError(err)

	msg := flatbuf.GetRootAsMessage(data.DataHeader, 0)
	t.Require().Equal(flatbuf.MessageHeaderSchema, msg.HeaderType())

	fields, ok := parseFlatbufferSchemaFields(msg)
	t.Require().True(ok)
	t.Require().Len(fields, len(expectedFields))

	assertSchemaMatchesFields(t, fields, expectedFields)
}

func assertSchemaMatchesFields(t *tester.Tester, fields []flatbuf.Field, expectedFields []field) {
	for _, expectedField := range expectedFields {
		field, found := matchFieldByName(fields, expectedField.Name)
		t.Assert().Truef(found, "no matching field with expected name \"%s\" found in flatbuffer schema", expectedField.Name)
		if !found {
			continue
		}

		t.Assert().Equal(expectedField.Name, string(field.Name()))
		t.Assert().Equal(expectedField.Type, field.TypeType())
		t.Assert().Equal(expectedField.Nullable, field.Nullable())
		// TODO: metadata?
	}
}

func requireDrainStream[E any, S interface {
	Recv() (E, error)
}](t *tester.Tester, stream S, eval func(*tester.Tester, E)) {
	for {
		data, err := stream.Recv()
		if err == io.EOF {
			break
		}
		t.Require().NoError(err)

		if eval != nil {
			eval(t, data)
		}
	}
}

func requireSchemaResultMatchesFields(t *tester.Tester, res *flight.SchemaResult, expectedFields []field) {
	metadata := extractFlatbufferPayload(res.Schema)

	msg := flatbuf.GetRootAsMessage(metadata, 0)
	t.Require().Equal(flatbuf.MessageHeaderSchema, msg.HeaderType())

	fields, ok := parseFlatbufferSchemaFields(msg)
	t.Require().True(ok)
	t.Require().Len(fields, len(expectedFields))

	assertSchemaMatchesFields(t, fields, expectedFields)
}
