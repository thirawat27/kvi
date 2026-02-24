# Kvi (Kinetic Virtual Index)

**Version:** 1.0.0
**Language:** Go 1.21+
**License:** MIT

Kvi คือฐานข้อมูลแบบ Multi-Modal ที่ผสมผสานข้อดีของ SQLite, Redis, PostgreSQL, DuckDB และ Pinecone ไว้ในระบบเดียว พร้อมรองรับทุกภาษาโปรแกรมผ่าน gRPC

## 🚀 คุณสมบัติหลัก

### Multi-Mode Engine
- **Memory** - Pure in-memory (เร็วที่สุด, สำหรับ cache/session)
- **Disk** - Persistent with WAL (ปลอดภัยที่สุด, ACID compliant)
- **Columnar** - Analytics-optimized (สำหรับ OLAP, aggregations)
- **Vector** - AI/ML similarity search (HNSW algorithm)
- **Hybrid** - Auto-switch based on workload

### ความสามารถขั้นสูง
- 🔄 **MVCC** - Multi-Version Concurrency Control สำหรับ time-travel queries
- 📡 **Pub/Sub** - Real-time messaging แบบ Redis-style
- 🔒 **WAL** - Write-Ahead Logging สำหรับ durability
- 📊 **SQL-like Query** - รองรับ SQL-like syntax
- 🌐 **Cross-Language** - รองรับทุกภาษาผ่าน gRPC, REST API

### 🔌 Cross-Language Support (100% Compatible)
| Language | SDK | gRPC | REST | Status |
|----------|-----|------|------|--------|
| **Go** | Native | ✅ | ✅ | ✅ Ready |
| **Python** | `pip install kvi` | ✅ | ✅ | ✅ Ready |
| **JavaScript/Node.js** | `npm install kvi` | ✅ | ✅ | ✅ Ready |
| **Java** | Maven/Gradle | ✅ | ✅ | 🔧 Coming |
| **C/C++** | Native | ✅ | - | 🔧 Coming |
| **Rust** | Cargo | ✅ | ✅ | 🔧 Coming |
| **Ruby** | Gem | ✅ | ✅ | 🔧 Coming |
| **PHP** | Composer | - | ✅ | ✅ Ready |

## 📦 การติดตั้ง

### Go
```bash
go get github.com/thirawat27/kvi
```

### Python
```bash
pip install kvi
```

### JavaScript/Node.js
```bash
npm install @kvi/sdk
```

## 🔧 การใช้งานเบื้องต้น

### Go (Native)
```go
package main

import (
    "context"
    "fmt"
    "github.com/thirawat27/kvi"
)

func main() {
    // In-memory mode
    db, _ := kvi.OpenMemory()
    defer db.Close()

    ctx := context.Background()

    // Set value
    db.Set(ctx, "user:1", "John Doe")

    // Get value
    val, _ := db.GetString(ctx, "user:1")
    fmt.Println(val) // John Doe
    
    // Vector search
    db.SetVector(ctx, "doc1", []float32{0.1, 0.2, 0.3}, map[string]interface{}{
        "title": "Document 1",
    })
    
    ids, scores, _ := db.VectorSearch([]float32{0.15, 0.25, 0.35}, 10)
}
```

### Python
```python
from kvi import KviClient

# Connect to server
client = KviClient('localhost:50051')

# Basic CRUD
client.put('user:1', {'name': 'John', 'age': 30})
record = client.get('user:1')
print(record.data)  # {'name': 'John', 'age': 30}

# Vector search
client.vector_add('doc1', [0.1, 0.2, 0.3], {'title': 'Document 1'})
results = client.vector_search([0.15, 0.25, 0.35], k=10)

# SQL-like query
records = client.query("SELECT * FROM users WHERE id = user:1")

client.close()
```

### JavaScript/Node.js
```javascript
const { KviClient } = require('@kvi/sdk');

async function main() {
    const client = new KviClient('localhost:50051');
    
    // Basic CRUD
    await client.put('user:1', { name: 'John', age: 30 });
    const record = await client.get('user:1');
    console.log(record.data);
    
    // Vector search
    await client.vectorAdd('doc1', [0.1, 0.2, 0.3], { title: 'Document 1' });
    const results = await client.vectorSearch([0.15, 0.25, 0.35], 10);
    
    client.close();
}

main();
```

### REST API (ทุกภาษา)
```bash
# Put
curl -X POST http://localhost:8080/api/v1/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:1", "data": {"name": "John"}}'

# Get
curl http://localhost:8080/api/v1/get?key=user:1

# Vector Search
curl -X POST http://localhost:8080/api/v1/vector/search \
  -H "Content-Type: application/json" \
  -d '{"vector": [0.1, 0.2, 0.3], "k": 10}'

# Query
curl -X POST http://localhost:8080/api/v1/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT * FROM users LIMIT 10"}'
```

## 🖥️ เริ่มต้น Server

### HTTP + gRPC Server
```bash
# HTTP server (port 8080)
go run ./cmd/kvi --mode memory --port 8080

# With disk persistence
go run ./cmd/kvi --mode disk --dir ./data --port 8080

# gRPC server (port 50051)
go run ./cmd/kvi --mode hybrid --grpc-port 50051 --port 8080
```

### CLI Commands
```bash
# Start server
kvi --mode memory --port 8080

# Execute single query
kvi --query "SELECT * FROM users LIMIT 10"

# Create backup
kvi --backup backup.json

# Restore from backup
kvi --restore backup.json

# Show version
kvi --version
```

## 📁 Project Structure

```
kvi/
├── cmd/
│   └── kvi/              # CLI tool
├── internal/
│   ├── engine/           # Core storage engine (MVCC, B-tree)
│   ├── wal/              # Write-Ahead Logging
│   ├── columnar/         # Column-oriented storage
│   ├── vector/           # HNSW vector index
│   ├── sql/              # SQL parser & planner
│   └── pubsub/           # Real-time messaging
├── pkg/
│   ├── api/              # REST API
│   ├── grpc/             # gRPC service
│   ├── types/            # Core types
│   └── config/           # Configuration
├── proto/
│   └── kvi.proto         # Protobuf definitions
├── sdk/
│   ├── python/           # Python SDK
│   ├── javascript/       # JavaScript SDK
│   └── rust/             # Rust SDK (coming soon)
├── tests/                # Integration tests
├── kvi.go                # Main package
└── go.mod
```

## ⚙️ Configuration

```go
config := &config.Config{
    Mode:              types.ModeHybrid,
    DataDir:           "./data",
    WALPath:           "./data/wal.log",
    MaxMemoryMB:       1024,
    CacheSizeMB:       256,
    MaxConnections:    1000,
    QueryTimeout:      30 * time.Second,
    VectorDimensions:  384,
    HNSWM:             16,
    HNSWEf:            200,
    EnableWAL:         true,
    EnablePubSub:      true,
    EnableGRPC:        true,
    GRPCPort:          50051,
    HTTPPort:          8080,
    Compression:       true,
}
```

## 🏎️ Performance Tips

1. **เลือก Mode ให้ถูกต้อง:**
   - `Memory` → Cache, Session store, Real-time leaderboard
   - `Disk` → Primary database, Financial records
   - `Columnar` → Analytics, Logs, Time-series
   - `Vector` → Recommendation, Semantic search, RAG

2. **Connection Pooling:**
   - gRPC มี built-in connection pooling
   - ใช้ connection pool สำหรับ HTTP clients

3. **Batch Operations:**
   ```python
   # ใช้ batch_put แทนการ put ทีละอัน
   client.batch_put({
       'key1': {'data': 1},
       'key2': {'data': 2},
       'key3': {'data': 3},
   })
   ```

## 📊 Benchmarks

```bash
go test ./tests/... -bench=. -benchmem

# Results (Memory mode, M1 MacBook):
# BenchmarkPut-8        500000    2400 ns/op    128 B/op    3 allocs/op
# BenchmarkGet-8       2000000     680 ns/op     32 B/op    1 allocs/op
# BenchmarkVectorSearch-8   10000  120000 ns/op  8192 B/op   12 allocs/op
```

## 🔐 Security

- TLS encryption สำหรับ gRPC connections
- API key authentication
- Checksum verification สำหรับ data integrity

## 📜 API Reference

### gRPC/Protobuf
ดู `proto/kvi.proto` สำหรับ full API definition

### REST Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/get?key=<key>` | Get record |
| POST | `/api/v1/put` | Put record |
| DELETE | `/api/v1/delete?key=<key>` | Delete record |
| GET | `/api/v1/scan` | Scan records |
| POST | `/api/v1/batch` | Batch insert |
| POST | `/api/v1/query` | Execute SQL-like query |
| POST | `/api/v1/vector/add` | Add vector |
| POST | `/api/v1/vector/search` | Search vectors |
| POST | `/api/v1/pub` | Publish message |
| GET | `/api/v1/sub` | Subscribe (SSE) |
| GET | `/api/v1/stats` | Get statistics |
| GET | `/health` | Health check |

## 🗺️ Roadmap

- [x] Core Engine (Memory/Disk/Hybrid)
- [x] WAL & ACID compliance
- [x] Columnar Storage
- [x] Vector Search (HNSW)
- [x] Pub/Sub messaging
- [x] SQL-like Query
- [x] gRPC API (cross-language)
- [x] Python SDK
- [x] JavaScript SDK
- [ ] Distributed Mode (Raft consensus)
- [ ] SQL JOINs & Subqueries
- [ ] WebAssembly support
- [ ] Java SDK
- [ ] Rust SDK
- [ ] C/C++ SDK

## 🤝 Contributing

เรายินดีรับ contributions! โปรดอ่าน [CONTRIBUTING.md](CONTRIBUTING.md)

## 📄 License

MIT License - ดู [LICENSE](LICENSE) สำหรับรายละเอียด

## 👥 Authors

**Simpli Team**
- thirawat27
- sirayu-pn

---

<p align="center">
  <b>Kvi</b> - One Database to Rule Them All 🚀
</p>