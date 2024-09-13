package cases

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/apache/arrow/dev/flight-integration/scenario"
	"github.com/apache/arrow/dev/flight-integration/tester"

	"github.com/apache/arrow/go/v18/arrow/flight/gen/flight"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func init() {
	scenario.Register(
		scenario.Scenario{
			Name: "poll_flight_info",
			Steps: []scenario.ScenarioStep{
				{
					Name: "get_in_progress",
					ServerHandler: scenario.Handler{PollFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.PollInfo, error) {
						progress := 0.1

						return &flight.PollInfo{
							Info: &flight.FlightInfo{FlightDescriptor: fd},
							FlightDescriptor: &flight.FlightDescriptor{
								Type: flight.FlightDescriptor_CMD,
								Cmd:  []byte("poll"),
							},
							Progress:       &progress,
							ExpirationTime: timestamppb.New(time.Now().Add(time.Second * 10)),
						}, nil
					}},
				},
				{
					Name: "get_completed",
					ServerHandler: scenario.Handler{PollFlightInfo: func(ctx context.Context, fd *flight.FlightDescriptor) (*flight.PollInfo, error) {
						if fd.Type != flight.FlightDescriptor_CMD {
							return nil, fmt.Errorf("expected FlightDescriptor.Type to be CMD, found: %s", fd.Type)
						}
						if string(fd.Cmd) != "poll" {
							return nil, fmt.Errorf("expected FlightDescriptor.Cmd to be \"poll\", found: \"%s\"", fd.Cmd)
						}

						info := &flight.FlightInfo{FlightDescriptor: fd}
						progress := 1.0
						return &flight.PollInfo{
							Info:     info,
							Progress: &progress,
						}, nil
					}},
				},
			},
			RunClient: func(ctx context.Context, client flight.FlightServiceClient, t *tester.Tester) {
				info, err := client.PollFlightInfo(ctx, &flight.FlightDescriptor{Type: flight.FlightDescriptor_CMD, Cmd: []byte("heavy query")})
				t.Require().NoError(err)

				t.Assert().NotNilf(info.FlightDescriptor, "descriptor is missing: %s", info.String())
				t.Assert().NotNilf(info.Progress, "progress is missing: %s", info.String())
				t.Assert().NotNilf(info.ExpirationTime, "expiration time is missing: %s", info.String())
				t.Assert().Truef(0.0 <= *info.Progress && *info.Progress <= 1.0, "invalid progress: %s", info.String())

				info, err = client.PollFlightInfo(ctx, info.FlightDescriptor)
				t.Require().NoError(err)

				t.Assert().Nilf(info.FlightDescriptor, "retried but not finished yet: %s", info.String())
				t.Assert().NotNilf(info.Progress, "progress is missing in finished query: %s", info.String())
				t.Assert().Nilf(info.ExpirationTime, "expiration time must not be set for finished query: %s", info.String())
				t.Assert().Falsef(math.Abs(*info.Progress-1.0) > 1e-5, "progress for finished query isn't 1.0: %s", info.String())
			},
		},
	)
}
