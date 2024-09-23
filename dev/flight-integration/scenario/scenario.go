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

package scenario

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/arrow/dev/flight-integration/protocol/flight"
	"github.com/apache/arrow/dev/flight-integration/tester"
)

var (
	scenariosMu sync.RWMutex
	scenarios   = make(map[string]Scenario)
)

func Register(scenario Scenario) {
	scenariosMu.Lock()
	defer scenariosMu.Unlock()

	if _, dup := scenarios[scenario.Name]; dup {
		panic("scenario: RegisterScenario called twice for scenario " + scenario.Name)
	}
	scenarios[scenario.Name] = scenario
}

func Unregister(scenario string) {
	scenariosMu.Lock()
	defer scenariosMu.Unlock()

	_, found := scenarios[scenario]
	if !found {
		panic("scenario: cannot UnregisterScenario, scenario not found: " + scenario)
	}

	delete(scenarios, scenario)
}

func GetScenarios(names ...string) ([]Scenario, error) {
	if len(names) == 0 {
		return getAllScenarios()
	}

	res := make([]Scenario, len(names))

	scenariosMu.RLock()
	defer scenariosMu.RUnlock()

	for i, name := range names {
		scenario, ok := scenarios[name]
		if !ok {
			return nil, fmt.Errorf("scenario: unknown scenario %q", name)
		}

		res[i] = scenario
	}

	return res, nil
}

func getAllScenarios() ([]Scenario, error) {
	scenariosMu.RLock()
	defer scenariosMu.RUnlock()

	res := make([]Scenario, 0, len(scenarios))
	for _, scenario := range scenarios {
		res = append(res, scenario)
	}

	return res, nil
}

type Scenario struct {
	Name      string
	Steps     []ScenarioStep
	RunClient func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester)
}

type ScenarioStep struct {
	Name          string
	ServerHandler Handler
}

type Handler struct {
	DoAction       func(*flight.Action, flight.FlightService_DoActionServer) error
	DoExchange     func(flight.FlightService_DoExchangeServer) error
	DoGet          func(*flight.Ticket, flight.FlightService_DoGetServer) error
	DoPut          func(flight.FlightService_DoPutServer) error
	GetFlightInfo  func(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	GetSchema      func(context.Context, *flight.FlightDescriptor) (*flight.SchemaResult, error)
	Handshake      func(flight.FlightService_HandshakeServer) error
	ListActions    func(*flight.Empty, flight.FlightService_ListActionsServer) error
	ListFlights    func(*flight.Criteria, flight.FlightService_ListFlightsServer) error
	PollFlightInfo func(context.Context, *flight.FlightDescriptor) (*flight.PollInfo, error)
}
