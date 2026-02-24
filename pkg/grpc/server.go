package kvi_grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"github.com/thirawat27/kvi/internal/pubsub"
	"github.com/thirawat27/kvi/pkg/types"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type GrpcServer struct {
	UnimplementedKviServiceServer
	engine types.Engine
	hub    *pubsub.Hub
}

func NewGrpcServer(eng types.Engine, hub *pubsub.Hub) *GrpcServer {
	return &GrpcServer{
		engine: eng,
		hub:    hub,
	}
}

func (s *GrpcServer) Get(ctx context.Context, req *GetRequest) (*GetResponse, error) {
	rec, err := s.engine.Get(ctx, req.Key)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}

	dataBytes, _ := json.Marshal(rec.Data)

	return &GetResponse{
		Id:       rec.ID,
		DataJson: string(dataBytes),
	}, nil
}

func (s *GrpcServer) Put(ctx context.Context, req *PutRequest) (*PutResponse, error) {
	var data map[string]interface{}
	if err := json.Unmarshal([]byte(req.DataJson), &data); err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid json data")
	}

	record := &types.Record{
		ID:   req.Key,
		Data: data,
	}

	if err := s.engine.Put(ctx, req.Key, record); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &PutResponse{Success: true}, nil
}

func (s *GrpcServer) VectorSearch(ctx context.Context, req *VectorSearchRequest) (*VectorSearchResponse, error) {
	// Not fully implemented interface, but stubbed logically
	return nil, status.Error(codes.Unimplemented, "Vector search gRPC pending interface link")
}

// Stream Handles bidirectional streaming for pub/sub operations
func (s *GrpcServer) Stream(stream KviService_StreamServer) error {
	ctx := stream.Context()
	var clientID string

	// Receive the first registration message
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	clientID = req.Id
	if clientID == "" {
		clientID = fmt.Sprintf("anon-%d", ctx.Value("anonymous")) // simplified
	}

	var sub *pubsub.Subscriber
	if req.Channel != "" {
		sub = s.hub.Subscribe(req.Channel, clientID)
		defer s.hub.Unsubscribe(req.Channel, clientID)
	}

	errChan := make(chan error, 1)

	// Goroutine to send messages back to the client
	go func() {
		if sub != nil {
			for {
				msg, ok := sub.Receive()
				if !ok {
					errChan <- nil
					return
				}
				resp := &StreamResponse{
					Channel: msg.Channel,
					Payload: msg.Payload,
				}
				if err := stream.Send(resp); err != nil {
					errChan <- err
					return
				}
			}
		}
	}()

	// Read stream from client
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Bidi Stream error: %v", err)
			break
		}

		if req.PublishPayload != "" {
			s.hub.Publish(req.Channel, req.PublishPayload)
		}
	}

	return nil
}
