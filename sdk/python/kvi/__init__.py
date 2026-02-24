"""
Kvi Python SDK - Cross-language compatible database client
Version: 1.0.0
"""

import grpc
import json
from typing import Dict, List, Optional, Any, Iterator
from dataclasses import dataclass
from datetime import datetime

# Import generated protobuf classes
from . import kvi_pb2
from . import kvi_pb2_grpc


@dataclass
class Record:
    """Represents a database record"""
    id: str
    data: Dict[str, Any]
    vector: Optional[List[float]] = None
    version: int = 0
    ttl: Optional[datetime] = None
    checksum: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


@dataclass
class VectorResult:
    """Represents a vector search result"""
    id: str
    score: float
    record: Optional[Record] = None


class KviClient:
    """
    Kvi Database Client
    
    Usage:
        client = KviClient('localhost:50051')
        client.put('key', {'name': 'test', 'value': 123})
        record = client.get('key')
        print(record.data)
    """
    
    def __init__(self, address: str = 'localhost:50051', 
                 use_tls: bool = False,
                 api_key: Optional[str] = None):
        """
        Initialize Kvi client
        
        Args:
            address: Server address (default: localhost:50051)
            use_tls: Use TLS encryption
            api_key: Optional API key for authentication
        """
        self.address = address
        
        # Create gRPC channel
        if use_tls:
            credentials = grpc.ssl_channel_credentials()
            self.channel = grpc.secure_channel(address, credentials)
        else:
            self.channel = grpc.insecure_channel(address)
        
        # Create stub
        self.stub = kvi_pb2_grpc.KviServiceStub(self.channel)
        
        # Metadata for authentication
        self.metadata = []
        if api_key:
            self.metadata = [('api-key', api_key)]
    
    def close(self):
        """Close the client connection"""
        self.channel.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    # ==================== Basic CRUD ====================
    
    def get(self, key: str, as_of: Optional[int] = None) -> Optional[Record]:
        """
        Retrieve a record by key
        
        Args:
            key: The key to retrieve
            as_of: Optional timestamp for time-travel query
            
        Returns:
            Record if found, None otherwise
        """
        request = kvi_pb2.GetRequest(key=key, as_of=as_of or 0)
        response = self.stub.Get(request, metadata=self.metadata)
        
        if not response.found:
            return None
        
        return self._proto_to_record(response.record)
    
    def put(self, key: str, data: Dict[str, Any], 
            vector: Optional[List[float]] = None,
            ttl_seconds: Optional[int] = None) -> int:
        """
        Store a record
        
        Args:
            key: The key to store
            data: The data to store
            vector: Optional vector for similarity search
            ttl_seconds: Optional TTL in seconds
            
        Returns:
            Version number of the stored record
        """
        record = kvi_pb2.Record(
            id=key,
            data=self._dict_to_proto(data),
            vector=vector or [],
        )
        
        if ttl_seconds:
            record.ttl = int((datetime.now().timestamp() + ttl_seconds))
        
        request = kvi_pb2.PutRequest(key=key, record=record)
        response = self.stub.Put(request, metadata=self.metadata)
        
        return response.version
    
    def delete(self, key: str) -> bool:
        """
        Delete a record
        
        Args:
            key: The key to delete
            
        Returns:
            True if deleted successfully
        """
        request = kvi_pb2.DeleteRequest(key=key)
        response = self.stub.Delete(request, metadata=self.metadata)
        return response.success
    
    def scan(self, start: str = '', end: str = '', 
             limit: int = 100) -> List[Record]:
        """
        Scan records in a key range
        
        Args:
            start: Start key (inclusive)
            end: End key (exclusive)
            limit: Maximum number of records
            
        Returns:
            List of records
        """
        request = kvi_pb2.ScanRequest(start=start, end=end, limit=limit)
        
        records = []
        for proto_record in self.stub.Scan(request, metadata=self.metadata):
            records.append(self._proto_to_record(proto_record))
        
        return records
    
    def batch_put(self, entries: Dict[str, Dict[str, Any]]) -> int:
        """
        Store multiple records efficiently
        
        Args:
            entries: Dictionary of key -> data
            
        Returns:
            Number of records stored
        """
        proto_entries = {}
        for key, data in entries.items():
            proto_entries[key] = kvi_pb2.Record(
                id=key,
                data=self._dict_to_proto(data),
            )
        
        request = kvi_pb2.BatchPutRequest(entries=proto_entries)
        response = self.stub.BatchPut(request, metadata=self.metadata)
        
        return response.count
    
    # ==================== Vector Operations ====================
    
    def vector_add(self, key: str, vector: List[float],
                   metadata: Optional[Dict[str, Any]] = None) -> bool:
        """
        Add a vector to the index
        
        Args:
            key: Unique identifier for the vector
            vector: The vector (list of floats)
            metadata: Optional metadata
            
        Returns:
            True if added successfully
        """
        request = kvi_pb2.VectorAddRequest(
            key=key,
            vector=vector,
            metadata=self._dict_to_proto(metadata or {}),
        )
        response = self.stub.VectorAdd(request, metadata=self.metadata)
        return response.success
    
    def vector_search(self, query: List[float], k: int = 10) -> List[VectorResult]:
        """
        Search for similar vectors
        
        Args:
            query: Query vector
            k: Number of results
            
        Returns:
            List of search results with scores
        """
        request = kvi_pb2.VectorSearchRequest(query=query, k=k)
        response = self.stub.VectorSearch(request, metadata=self.metadata)
        
        results = []
        for result in response.results:
            record = None
            if result.HasField('record'):
                record = self._proto_to_record(result.record)
            
            results.append(VectorResult(
                id=result.id,
                score=result.score,
                record=record,
            ))
        
        return results
    
    # ==================== Pub/Sub ====================
    
    def publish(self, channel: str, data: bytes) -> bool:
        """
        Publish a message to a channel
        
        Args:
            channel: Channel name
            data: Message data
            
        Returns:
            True if published successfully
        """
        request = kvi_pb2.PublishRequest(channel=channel, data=data)
        response = self.stub.Publish(request, metadata=self.metadata)
        return response.success
    
    def subscribe(self, channel: str, subscriber_id: str) -> Iterator[dict]:
        """
        Subscribe to a channel
        
        Args:
            channel: Channel name
            subscriber_id: Unique subscriber ID
            
        Yields:
            Message dictionaries
        """
        request = kvi_pb2.SubscribeRequest(
            channel=channel,
            subscriber_id=subscriber_id,
        )
        
        for message in self.stub.Subscribe(request, metadata=self.metadata):
            yield {
                'id': message.id,
                'channel': message.channel,
                'data': message.data,
                'timestamp': datetime.fromtimestamp(message.timestamp),
            }
    
    # ==================== Query ====================
    
    def query(self, sql: str) -> List[Record]:
        """
        Execute a SQL-like query
        
        Args:
            sql: SQL query string
            
        Returns:
            List of records
        """
        request = kvi_pb2.QueryRequest(query=sql)
        response = self.stub.Query(request, metadata=self.metadata)
        
        if not response.success:
            raise Exception(response.error)
        
        return [self._proto_to_record(r) for r in response.records]
    
    # ==================== Stats & Health ====================
    
    def stats(self) -> dict:
        """
        Get database statistics
        
        Returns:
            Statistics dictionary
        """
        request = kvi_pb2.StatsRequest()
        response = self.stub.Stats(request, metadata=self.metadata)
        
        return {
            'records_total': response.records_total,
            'memory_used': response.memory_used,
            'disk_used': response.disk_used,
            'cache_hit_ratio': response.cache_hit_ratio,
            'avg_query_time_ns': response.avg_query_time_ns,
            'wal_size': response.wal_size,
            'mode': response.mode,
            'version': response.version,
        }
    
    def health(self) -> dict:
        """
        Check server health
        
        Returns:
            Health status dictionary
        """
        request = kvi_pb2.HealthRequest()
        response = self.stub.Health(request, metadata=self.metadata)
        
        return {
            'status': response.status,
            'timestamp': datetime.fromtimestamp(response.timestamp),
            'mode': response.mode,
        }
    
    # ==================== Helper Methods ====================
    
    def _proto_to_record(self, proto) -> Record:
        """Convert protobuf Record to Python Record"""
        ttl = None
        if proto.ttl > 0:
            ttl = datetime.fromtimestamp(proto.ttl)
        
        created_at = None
        if proto.created_at > 0:
            created_at = datetime.fromtimestamp(proto.created_at)
        
        updated_at = None
        if proto.updated_at > 0:
            updated_at = datetime.fromtimestamp(proto.updated_at)
        
        return Record(
            id=proto.id,
            data=self._proto_to_dict(proto.data),
            vector=list(proto.vector) if proto.vector else None,
            version=proto.version,
            ttl=ttl,
            checksum=proto.checksum,
            created_at=created_at,
            updated_at=updated_at,
        )
    
    def _proto_to_dict(self, proto_map) -> Dict[str, Any]:
        """Convert protobuf Value map to Python dict"""
        result = {}
        for key, value in proto_map.items():
            result[key] = self._proto_value_to_python(value)
        return result
    
    def _proto_value_to_python(self, value) -> Any:
        """Convert protobuf Value to Python value"""
        which = value.WhichOneof('value')
        
        if which == 'string_value':
            return value.string_value
        elif which == 'int_value':
            return value.int_value
        elif which == 'float_value':
            return value.float_value
        elif which == 'bool_value':
            return value.bool_value
        elif which == 'bytes_value':
            return value.bytes_value
        elif which == 'array_value':
            return [self._proto_value_to_python(v) for v in value.array_value.values]
        elif which == 'map_value':
            return self._proto_to_dict(value.map_value.values)
        
        return None
    
    def _dict_to_proto(self, data: Dict[str, Any]) -> Dict[str, kvi_pb2.Value]:
        """Convert Python dict to protobuf Value map"""
        result = {}
        for key, value in data.items():
            result[key] = self._python_to_proto_value(value)
        return result
    
    def _python_to_proto_value(self, value: Any) -> kvi_pb2.Value:
        """Convert Python value to protobuf Value"""
        proto_value = kvi_pb2.Value()
        
        if isinstance(value, str):
            proto_value.string_value = value
        elif isinstance(value, bool):
            proto_value.bool_value = value
        elif isinstance(value, int):
            proto_value.int_value = value
        elif isinstance(value, float):
            proto_value.float_value = value
        elif isinstance(value, bytes):
            proto_value.bytes_value = value
        elif isinstance(value, list):
            array = kvi_pb2.ValueArray()
            for item in value:
                array.values.append(self._python_to_proto_value(item))
            proto_value.array_value.CopyFrom(array)
        elif isinstance(value, dict):
            map_value = kvi_pb2.ValueMap()
            for k, v in value.items():
                map_value.values[k] = self._python_to_proto_value(v)
            proto_value.map_value.CopyFrom(map_value)
        
        return proto_value


# Convenience functions
def connect(address: str = 'localhost:50051', **kwargs) -> KviClient:
    """
    Create a new Kvi client connection
    
    Args:
        address: Server address
        **kwargs: Additional arguments passed to KviClient
        
    Returns:
        KviClient instance
    """
    return KviClient(address, **kwargs)


# Version
__version__ = '1.0.0'
__all__ = ['KviClient', 'Record', 'VectorResult', 'connect']