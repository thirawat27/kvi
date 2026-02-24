package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/thirawat27/kvi/internal/engine"
	"github.com/thirawat27/kvi/internal/pubsub"
	"github.com/thirawat27/kvi/internal/sql"
	"github.com/thirawat27/kvi/pkg/config"
	pb "github.com/thirawat27/kvi/pkg/grpc/kvipb"
	"github.com/thirawat27/kvi/pkg/types"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// KviGRPCServer implements the gRPC KviService
type KviGRPCServer struct {
	pb.UnimplementedKviServiceServer
	engine *engine.KviEngine
	hub    *pubsub.Hub
	parser *sql.Parser
	config *config.Config
}

// NewGRPCServer creates a new gRPC server
func NewGRPCServer(eng *engine.KviEngine, cfg *config.Config) *KviGRPCServer {
	return &KviGRPCServer{
		engine: eng,
		hub:    pubsub.NewHub(),
		parser: sql.NewParser(),
		config: cfg,
	}
}

// Get retrieves a record by key
func (s *KviGRPCServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	var record *types.Record
	var err error

	if req.AsOf > 0 {
		record, err = s.engine.GetAsOf(req.Key, req.AsOf)
	} else {
		record, err = s.engine.Get(ctx, req.Key)
	}

	if err != nil {
		return &pb.GetResponse{Found: false}, nil
	}

	return &pb.GetResponse{
		Found:  true,
		Record: typesRecordToProto(record),
	}, nil
}

// Put stores a record
func (s *KviGRPCServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	record := protoToTypesRecord(req.Record)
	if err := s.engine.Put(ctx, req.Key, record); err != nil {
		return &pb.PutResponse{Success: false}, err
	}

	return &pb.PutResponse{
		Success: true,
		Version: record.Version,
	}, nil
}

// Delete removes a record
func (s *KviGRPCServer) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	err := s.engine.Delete(ctx, req.Key)
	if err != nil && err != types.ErrKeyNotFound {
		return &pb.DeleteResponse{Success: false}, err
	}
	return &pb.DeleteResponse{Success: true}, nil
}

// Scan retrieves records in a key range
func (s *KviGRPCServer) Scan(req *pb.ScanRequest, stream pb.KviService_ScanServer) error {
	ctx := stream.Context()
	records, err := s.engine.Scan(ctx, req.Start, req.End, int(req.Limit))
	if err != nil {
		return err
	}

	for _, record := range records {
		if err := stream.Send(typesRecordToProto(record)); err != nil {
			return err
		}
	}

	return nil
}

// BatchPut stores multiple records efficiently
func (s *KviGRPCServer) BatchPut(ctx context.Context, req *pb.BatchPutRequest) (*pb.BatchPutResponse, error) {
	entries := make(map[string]*types.Record)
	for key, rec := range req.Entries {
		entries[key] = protoToTypesRecord(rec)
	}

	if err := s.engine.BatchPut(ctx, entries); err != nil {
		return &pb.BatchPutResponse{Success: false}, err
	}

	return &pb.BatchPutResponse{
		Success: true,
		Count:   int32(len(entries)),
	}, nil
}

// VectorAdd adds a vector to the index
func (s *KviGRPCServer) VectorAdd(ctx context.Context, req *pb.VectorAddRequest) (*pb.VectorAddResponse, error) {
	metadata := protoValueMapToInterface(req.Metadata)
	record := &types.Record{
		ID:     req.Key,
		Vector: req.Vector,
		Data:   metadata,
	}

	if err := s.engine.Put(ctx, req.Key, record); err != nil {
		return &pb.VectorAddResponse{Success: false}, err
	}

	return &pb.VectorAddResponse{Success: true}, nil
}

// VectorSearch performs similarity search
func (s *KviGRPCServer) VectorSearch(ctx context.Context, req *pb.VectorSearchRequest) (*pb.VectorSearchResponse, error) {
	ids, scores, err := s.engine.VectorSearch(req.Query, int(req.K))
	if err != nil {
		return &pb.VectorSearchResponse{}, err
	}

	results := make([]*pb.VectorResult, len(ids))
	for i, id := range ids {
		results[i] = &pb.VectorResult{
			Id:    id,
			Score: scores[i],
		}

		// Fetch record if available
		if record, err := s.engine.Get(ctx, id); err == nil {
			results[i].Record = typesRecordToProto(record)
		}
	}

	return &pb.VectorSearchResponse{Results: results}, nil
}

// Snapshot creates a database snapshot
func (s *KviGRPCServer) Snapshot(ctx context.Context, req *pb.SnapshotRequest) (*pb.SnapshotResponse, error) {
	snap, err := s.engine.Snapshot()
	if err != nil {
		return &pb.SnapshotResponse{}, err
	}

	data, err := json.Marshal(snap)
	if err != nil {
		return &pb.SnapshotResponse{}, err
	}

	return &pb.SnapshotResponse{
		SnapshotData: data,
		Checksum:     snap.Checksum,
		CreatedAt:    snap.CreatedAt.Unix(),
	}, nil
}

// Restore restores from a snapshot
func (s *KviGRPCServer) Restore(ctx context.Context, req *pb.RestoreRequest) (*pb.RestoreResponse, error) {
	var snap types.Snapshot
	if err := json.Unmarshal(req.SnapshotData, &snap); err != nil {
		return &pb.RestoreResponse{Success: false}, err
	}
	snap.Checksum = req.Checksum

	if err := s.engine.Restore(snap); err != nil {
		return &pb.RestoreResponse{Success: false}, err
	}

	return &pb.RestoreResponse{Success: true}, nil
}

// Publish publishes a message to a channel
func (s *KviGRPCServer) Publish(ctx context.Context, req *pb.PublishRequest) (*pb.PublishResponse, error) {
	// Create channel if not exists
	if _, err := s.hub.GetChannel(req.Channel); err != nil {
		s.hub.CreateChannel(req.Channel, 100)
	}

	if err := s.hub.Publish(req.Channel, req.Data); err != nil {
		return &pb.PublishResponse{Success: false}, err
	}

	return &pb.PublishResponse{Success: true}, nil
}

// Subscribe subscribes to a channel
func (s *KviGRPCServer) Subscribe(req *pb.SubscribeRequest, stream pb.KviService_SubscribeServer) error {
	// Create channel if not exists
	if _, err := s.hub.GetChannel(req.Channel); err != nil {
		s.hub.CreateChannel(req.Channel, 100)
	}

	sub, err := s.hub.Subscribe(req.Channel, req.SubscriberId)
	if err != nil {
		return err
	}
	defer s.hub.Unsubscribe(req.Channel, req.SubscriberId)

	for {
		select {
		case <-stream.Context().Done():
			return nil
		case msg, ok := <-sub.C:
			if !ok {
				return nil
			}
			data, _ := msg.Data.([]byte)
			if err := stream.Send(&pb.Message{
				Id:        msg.ID,
				Channel:   msg.Channel,
				Data:      data,
				Timestamp: msg.Timestamp.Unix(),
			}); err != nil {
				return err
			}
		}
	}
}

// Query executes a SQL-like query
func (s *KviGRPCServer) Query(ctx context.Context, req *pb.QueryRequest) (*pb.QueryResponse, error) {
	stmt, err := s.parser.Parse(req.Query)
	if err != nil {
		return &pb.QueryResponse{Success: false, Error: err.Error()}, nil
	}

	// Execute query based on statement type
	switch stmt.Type {
	case sql.Select:
		return s.executeSelect(ctx, stmt)
	case sql.Insert:
		return s.executeInsert(ctx, stmt)
	case sql.Delete:
		return s.executeDelete(ctx, stmt)
	case sql.VectorSearch:
		return s.executeVectorSearch(ctx, stmt)
	default:
		return &pb.QueryResponse{Success: false, Error: "unsupported statement type"}, nil
	}
}

// Stats returns database statistics
func (s *KviGRPCServer) Stats(ctx context.Context, req *pb.StatsRequest) (*pb.StatsResponse, error) {
	stats := s.engine.Stats()
	return &pb.StatsResponse{
		RecordsTotal:   stats.RecordsTotal,
		MemoryUsed:     stats.MemoryUsed,
		DiskUsed:       stats.DiskUsed,
		CacheHitRatio:  stats.CacheHitRatio,
		AvgQueryTimeNs: int64(stats.AvgQueryTime),
		WalSize:        stats.WALSize,
		Mode:           s.config.Mode.String(),
		Version:        "1.0.0",
	}, nil
}

// Health checks the server health
func (s *KviGRPCServer) Health(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Status:    "healthy",
		Timestamp: time.Now().Unix(),
		Mode:      s.config.Mode.String(),
	}, nil
}

// Helper functions

func typesRecordToProto(r *types.Record) *pb.Record {
	if r == nil {
		return nil
	}

	var ttl int64
	if r.TTL != nil {
		ttl = r.TTL.Unix()
	}

	return &pb.Record{
		Id:        r.ID,
		Data:      interfaceMapToProtoValueMap(r.Data),
		Vector:    r.Vector,
		Version:   r.Version,
		Ttl:       ttl,
		Checksum:  r.Checksum,
		CreatedAt: r.CreatedAt.Unix(),
		UpdatedAt: r.UpdatedAt.Unix(),
	}
}

func protoToTypesRecord(r *pb.Record) *types.Record {
	if r == nil {
		return nil
	}

	record := &types.Record{
		ID:        r.Id,
		Data:      protoValueMapToInterfaceMap(r.Data),
		Vector:    r.Vector,
		Version:   r.Version,
		Checksum:  r.Checksum,
		CreatedAt: time.Unix(r.CreatedAt, 0),
		UpdatedAt: time.Unix(r.UpdatedAt, 0),
	}

	if r.Ttl > 0 {
		ttl := time.Unix(r.Ttl, 0)
		record.TTL = &ttl
	}

	return record
}

func interfaceMapToProtoValueMap(m map[string]interface{}) map[string]*pb.Value {
	result := make(map[string]*pb.Value)
	for k, v := range m {
		result[k] = interfaceToProtoValue(v)
	}
	return result
}

func protoValueMapToInterfaceMap(m map[string]*pb.Value) map[string]interface{} {
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = protoValueToInterface(v)
	}
	return result
}

func protoValueMapToInterface(m map[string]*pb.Value) map[string]interface{} {
	if m == nil {
		return nil
	}
	result := make(map[string]interface{})
	for k, v := range m {
		result[k] = protoValueToInterface(v)
	}
	return result
}

func interfaceToProtoValue(v interface{}) *pb.Value {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case string:
		return &pb.Value{Value: &pb.Value_StringValue{StringValue: val}}
	case int:
		return &pb.Value{Value: &pb.Value_IntValue{IntValue: int64(val)}}
	case int64:
		return &pb.Value{Value: &pb.Value_IntValue{IntValue: val}}
	case float64:
		return &pb.Value{Value: &pb.Value_FloatValue{FloatValue: val}}
	case float32:
		return &pb.Value{Value: &pb.Value_FloatValue{FloatValue: float64(val)}}
	case bool:
		return &pb.Value{Value: &pb.Value_BoolValue{BoolValue: val}}
	case []byte:
		return &pb.Value{Value: &pb.Value_BytesValue{BytesValue: val}}
	default:
		// Fallback to string
		return &pb.Value{Value: &pb.Value_StringValue{StringValue: fmt.Sprintf("%v", val)}}
	}
}

func protoValueToInterface(v *pb.Value) interface{} {
	if v == nil {
		return nil
	}

	switch val := v.Value.(type) {
	case *pb.Value_StringValue:
		return val.StringValue
	case *pb.Value_IntValue:
		return val.IntValue
	case *pb.Value_FloatValue:
		return val.FloatValue
	case *pb.Value_BoolValue:
		return val.BoolValue
	case *pb.Value_BytesValue:
		return val.BytesValue
	case *pb.Value_ArrayValue:
		arr := make([]interface{}, len(val.ArrayValue.Values))
		for i, item := range val.ArrayValue.Values {
			arr[i] = protoValueToInterface(item)
		}
		return arr
	case *pb.Value_MapValue:
		m := make(map[string]interface{})
		for k, item := range val.MapValue.Values {
			m[k] = protoValueToInterface(item)
		}
		return m
	default:
		return nil
	}
}

// Query execution helpers
func (s *KviGRPCServer) executeSelect(ctx context.Context, stmt *sql.Statement) (*pb.QueryResponse, error) {
	if stmt.Where != nil && (stmt.Where.Column == "id" || stmt.Where.Column == "key") {
		key := fmt.Sprintf("%v", stmt.Where.Value)
		record, err := s.engine.Get(ctx, key)
		if err != nil {
			return &pb.QueryResponse{Success: true, Records: []*pb.Record{}}, nil
		}
		return &pb.QueryResponse{Success: true, Records: []*pb.Record{typesRecordToProto(record)}}, nil
	}

	limit := stmt.Limit
	if limit <= 0 {
		limit = 100
	}

	records, err := s.engine.Scan(ctx, "", "", limit)
	if err != nil {
		return &pb.QueryResponse{Success: false, Error: err.Error()}, nil
	}

	protoRecords := make([]*pb.Record, len(records))
	for i, r := range records {
		protoRecords[i] = typesRecordToProto(r)
	}

	return &pb.QueryResponse{Success: true, Records: protoRecords}, nil
}

func (s *KviGRPCServer) executeInsert(ctx context.Context, stmt *sql.Statement) (*pb.QueryResponse, error) {
	record := &types.Record{Data: make(map[string]interface{})}
	for i, col := range stmt.Columns {
		if i < len(stmt.Values) {
			record.Data[col] = stmt.Values[i]
		}
	}

	key := stmt.Table
	if len(stmt.Columns) > 0 && len(stmt.Values) > 0 {
		key = fmt.Sprintf("%v", stmt.Values[0])
	}
	record.ID = key

	if err := s.engine.Put(ctx, key, record); err != nil {
		return &pb.QueryResponse{Success: false, Error: err.Error()}, nil
	}

	return &pb.QueryResponse{Success: true, Records: []*pb.Record{typesRecordToProto(record)}}, nil
}

func (s *KviGRPCServer) executeDelete(ctx context.Context, stmt *sql.Statement) (*pb.QueryResponse, error) {
	if stmt.Where != nil && (stmt.Where.Column == "id" || stmt.Where.Column == "key") {
		key := fmt.Sprintf("%v", stmt.Where.Value)
		if err := s.engine.Delete(ctx, key); err != nil {
			return &pb.QueryResponse{Success: false, Error: err.Error()}, nil
		}
		return &pb.QueryResponse{Success: true}, nil
	}
	return &pb.QueryResponse{Success: false, Error: "DELETE requires WHERE clause on id/key"}, nil
}

func (s *KviGRPCServer) executeVectorSearch(ctx context.Context, stmt *sql.Statement) (*pb.QueryResponse, error) {
	if stmt.VectorQuery == nil {
		return &pb.QueryResponse{Success: false, Error: "invalid vector search"}, nil
	}

	ids, _, err := s.engine.VectorSearch(stmt.VectorQuery.Vector, stmt.VectorQuery.K)
	if err != nil {
		return &pb.QueryResponse{Success: false, Error: err.Error()}, nil
	}

	records := make([]*pb.Record, 0, len(ids))
	for _, id := range ids {
		if record, err := s.engine.Get(ctx, id); err == nil {
			records = append(records, typesRecordToProto(record))
		}
	}

	return &pb.QueryResponse{Success: true, Records: records}, nil
}

// StartGRPCServer starts the gRPC server
func StartGRPCServer(eng *engine.KviEngine, cfg *config.Config, port int) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}

	grpcServer := grpc.NewServer()
	pb.RegisterKviServiceServer(grpcServer, NewGRPCServer(eng, cfg))
	reflection.Register(grpcServer)

	return grpcServer.Serve(lis)
	pb.RegisterKviServiceServer(grpcServer, NewGRPCServer(eng, cfg))
	reflection.Register(grpcServer)

	return grpcServer.Serve(lis)
}
