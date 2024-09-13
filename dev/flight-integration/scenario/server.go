package scenario

import (
	"context"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

func NewScenarioServer(scenarios []Scenario) *scenarioServer {
	server := scenarioServer{
		scenarios:     scenarios,
		clientDoneCh:  make(chan struct{}, 1),
		serverReadyCh: make(chan struct{}, 1),
	}
	server.serverReadyCh <- struct{}{}

	// TODO: maybe provide shutdown callback
	go func() {
		for {
			<-server.clientDoneCh

			if !server.expectingClientReset {
				server.FinishScenario()
			}
			server.expectingClientReset = false

			server.serverReadyCh <- struct{}{}
		}
	}()

	return &server
}

type scenarioServer struct {
	flight.UnimplementedFlightServiceServer

	scenarios []Scenario

	curScenario, curStep               int
	clientDoneCh, serverReadyCh        chan struct{}
	expectingClientReset, failed, done bool
}

func (s *scenarioServer) CurrentScenario() (Scenario, error) {
	if s.curScenario < len(s.scenarios) {
		return s.scenarios[s.curScenario], nil
	}
	return Scenario{}, fmt.Errorf("no more scenarios to execute, all %d have already run", len(s.scenarios))
}

func (s *scenarioServer) CurrentStep() (Scenario, ScenarioStep, error) {
	scenario, err := s.CurrentScenario()
	if err != nil {
		return Scenario{}, ScenarioStep{}, err
	}

	if s.curStep < len(scenario.Steps) {
		return scenario, scenario.Steps[s.curStep], nil
	}

	return scenario, ScenarioStep{}, status.Errorf(codes.OutOfRange, "call number %d for scenario %s, only %d steps expected", s.curStep, scenario.Name, len(scenario.Steps))
}

func (s *scenarioServer) StartStep() (Scenario, ScenarioStep, error) {
	if s.done {
		return Scenario{}, ScenarioStep{}, fmt.Errorf("no more scenarios left to run")
	}

	if s.expectingClientReset {
		return Scenario{},
			ScenarioStep{},
			status.Errorf(codes.FailedPrecondition, "finished scenario \"%s\", waiting to start scenario \"%s\": %s", s.scenarios[s.curScenario-1].Name, s.scenarios[s.curScenario].Name, ErrExpectedClientDisconnect)
	}

	return s.CurrentStep()
}

func (s *scenarioServer) FinishStep() {
	s.curStep++
	if s.failed || s.curStep == len(s.scenarios[s.curScenario].Steps) {
		s.FinishScenario()
	}
}

func (s *scenarioServer) FinishScenario() {
	s.curStep = 0
	s.expectingClientReset = true
	s.failed = false

	s.curScenario++
	if s.curScenario == len(s.scenarios) {
		log.Println("All scenarios completed")
		s.done = true
	}
}

func (s *scenarioServer) Error(scenario, step, method string) error {
	s.failed = true
	return status.Errorf(codes.PermissionDenied, "%s should not be called for scenario: %s, step: %s", method, scenario, step)
}

// DoAction implements flight.FlightServiceServer.
func (s *scenarioServer) DoAction(action *flight.Action, stream flight.FlightService_DoActionServer) error {
	scenario, step, err := s.StartStep()
	if err != nil {
		return err
	}
	defer s.FinishStep()

	if step.ServerHandler.DoAction == nil {
		return s.Error(scenario.Name, step.Name, "DoAction")
	}

	return step.ServerHandler.DoAction(action, stream)
}

// DoExchange implements flight.FlightServiceServer.
func (s *scenarioServer) DoExchange(stream flight.FlightService_DoExchangeServer) error {
	scenario, step, err := s.StartStep()
	if err != nil {
		return err
	}
	defer s.FinishStep()

	if step.ServerHandler.DoExchange == nil {
		return s.Error(scenario.Name, step.Name, "DoExchange")
	}

	return step.ServerHandler.DoExchange(stream)
}

// DoGet implements flight.FlightServiceServer.
func (s *scenarioServer) DoGet(ticket *flight.Ticket, stream flight.FlightService_DoGetServer) error {
	scenario, step, err := s.StartStep()
	if err != nil {
		return err
	}
	defer s.FinishStep()

	if step.ServerHandler.DoGet == nil {
		return s.Error(scenario.Name, step.Name, "DoGet")
	}

	return step.ServerHandler.DoGet(ticket, stream)
}

// DoPut implements flight.FlightServiceServer.
func (s *scenarioServer) DoPut(stream flight.FlightService_DoPutServer) error {
	scenario, step, err := s.StartStep()
	if err != nil {
		return err
	}
	defer s.FinishStep()

	if step.ServerHandler.DoPut == nil {
		return s.Error(scenario.Name, step.Name, "DoPut")
	}

	return step.ServerHandler.DoPut(stream)
}

// GetFlightInfo implements flight.FlightServiceServer.
func (s *scenarioServer) GetFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	scenario, step, err := s.StartStep()
	if err != nil {
		return nil, err
	}
	defer s.FinishStep()

	if step.ServerHandler.GetFlightInfo == nil {
		return nil, s.Error(scenario.Name, step.Name, "GetFlightInfo")
	}

	return step.ServerHandler.GetFlightInfo(ctx, desc)
}

// GetSchema implements flight.FlightServiceServer.
func (s *scenarioServer) GetSchema(ctx context.Context, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	scenario, step, err := s.StartStep()
	if err != nil {
		return nil, err
	}
	defer s.FinishStep()

	if step.ServerHandler.GetSchema == nil {
		return nil, s.Error(scenario.Name, step.Name, "GetSchema")
	}

	return step.ServerHandler.GetSchema(ctx, desc)
}

// Handshake implements flight.FlightServiceServer.
func (s *scenarioServer) Handshake(stream flight.FlightService_HandshakeServer) error {
	scenario, step, err := s.StartStep()
	if err != nil {
		return err
	}
	defer s.FinishStep()

	if step.ServerHandler.Handshake == nil {
		return s.Error(scenario.Name, step.Name, "Handshake")
	}

	return step.ServerHandler.Handshake(stream)
}

// ListActions implements flight.FlightServiceServer.
func (s *scenarioServer) ListActions(in *flight.Empty, stream flight.FlightService_ListActionsServer) error {
	scenario, step, err := s.StartStep()
	if err != nil {
		return err
	}
	defer s.FinishStep()

	if step.ServerHandler.ListActions == nil {
		return s.Error(scenario.Name, step.Name, "ListActions")
	}

	return step.ServerHandler.ListActions(in, stream)
}

// ListFlights implements flight.FlightServiceServer.
func (s *scenarioServer) ListFlights(in *flight.Criteria, stream flight.FlightService_ListFlightsServer) error {
	scenario, step, err := s.StartStep()
	if err != nil {
		return err
	}
	defer s.FinishStep()

	if step.ServerHandler.ListFlights == nil {
		return s.Error(scenario.Name, step.Name, "ListFlights")
	}

	return step.ServerHandler.ListFlights(in, stream)
}

// PollFlightInfo implements flight.FlightServiceServer.
func (s *scenarioServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	scenario, step, err := s.StartStep()
	if err != nil {
		return nil, err
	}
	defer s.FinishStep()

	if step.ServerHandler.PollFlightInfo == nil {
		return nil, s.Error(scenario.Name, step.Name, "PollFlightInfo")
	}

	return step.ServerHandler.PollFlightInfo(ctx, desc)
}

// HandleConn implements stats.Handler.
func (s *scenarioServer) HandleConn(ctx context.Context, connStats stats.ConnStats) {
	switch connStats.(type) {
	case *stats.ConnBegin:
		log.Println("Begin conn")
		select {
		case <-s.serverReadyCh:
			log.Println("conn acquired")
		default:
			log.Fatal("invalid state: only one client may connect to the integration server at a time")
		}

	case *stats.ConnEnd:
		log.Println("End conn")
		select {
		case s.clientDoneCh <- struct{}{}:
			log.Println("conn restored")
		default:
			log.Fatal("invalid state: server recieved multiple disconnects but only supports one client")
		}
	}
}

// HandleRPC implements stats.Handler.
func (s *scenarioServer) HandleRPC(ctx context.Context, rpcStats stats.RPCStats) {}

// TagConn implements stats.Handler.
func (s *scenarioServer) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

// TagRPC implements stats.Handler.
func (s *scenarioServer) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return ctx
}

var (
	_ flight.FlightServiceServer = (*scenarioServer)(nil)
	_ stats.Handler              = (*scenarioServer)(nil)
)
