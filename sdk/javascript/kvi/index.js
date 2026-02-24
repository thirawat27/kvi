/**
 * Kvi JavaScript SDK - Cross-language compatible database client
 * Version: 1.0.0
 * 
 * Works with Node.js and browsers
 */

const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// Load protobuf definition
const PROTO_PATH = path.join(__dirname, '..', '..', '..', 'proto', 'kvi.proto');

let kviPackage = null;

function loadProto() {
    if (kviPackage) return kviPackage;
    
    const packageDefinition = protoLoader.loadSync(PROTO_PATH, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
    });
    
    kviPackage = grpc.loadPackageDefinition(packageDefinition).kvi;
    return kviPackage;
}

/**
 * Represents a database record
 */
class Record {
    constructor(options = {}) {
        this.id = options.id || '';
        this.data = options.data || {};
        this.vector = options.vector || null;
        this.version = options.version || 0;
        this.ttl = options.ttl || null;
        this.checksum = options.checksum || 0;
        this.createdAt = options.createdAt || null;
        this.updatedAt = options.updatedAt || null;
    }
    
    static fromProto(proto) {
        if (!proto) return null;
        
        return new Record({
            id: proto.id,
            data: protoValueMapToObject(proto.data),
            vector: proto.vector && proto.vector.length > 0 ? [...proto.vector] : null,
            version: parseInt(proto.version) || 0,
            ttl: proto.ttl > 0 ? new Date(proto.ttl * 1000) : null,
            checksum: proto.checksum,
            createdAt: proto.created_at > 0 ? new Date(proto.created_at * 1000) : null,
            updatedAt: proto.updated_at > 0 ? new Date(proto.updated_at * 1000) : null,
        });
    }
}

/**
 * Represents a vector search result
 */
class VectorResult {
    constructor(options = {}) {
        this.id = options.id || '';
        this.score = options.score || 0;
        this.record = options.record || null;
    }
}

/**
 * Kvi Database Client
 * 
 * @example
 * const client = new KviClient('localhost:50051');
 * await client.put('key', { name: 'test' });
 * const record = await client.get('key');
 * console.log(record.data);
 */
class KviClient {
    /**
     * Create a new Kvi client
     * @param {string} address - Server address (default: localhost:50051)
     * @param {Object} options - Client options
     */
    constructor(address = 'localhost:50051', options = {}) {
        this.address = address;
        this.options = options;
        
        const kvi = loadProto();
        
        // Create gRPC client
        const credentials = options.useTls
            ? grpc.credentials.createSsl()
            : grpc.credentials.createInsecure();
        
        this.client = new kvi.KviService(address, credentials);
        
        // Metadata for authentication
        this.metadata = new grpc.Metadata();
        if (options.apiKey) {
            this.metadata.add('api-key', options.apiKey);
        }
    }
    
    /**
     * Close the client connection
     */
    close() {
        this.client.close();
    }
    
    // ==================== Basic CRUD ====================
    
    /**
     * Get a record by key
     * @param {string} key - The key to retrieve
     * @param {number} asOf - Optional timestamp for time-travel query
     * @returns {Promise<Record|null>}
     */
    async get(key, asOf = null) {
        return new Promise((resolve, reject) => {
            const request = { key, as_of: asOf || 0 };
            
            this.client.Get(request, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(response.found ? Record.fromProto(response.record) : null);
                }
            });
        });
    }
    
    /**
     * Store a record
     * @param {string} key - The key to store
     * @param {Object} data - The data to store
     * @param {Object} options - Additional options (vector, ttlSeconds)
     * @returns {Promise<number>} Version number
     */
    async put(key, data, options = {}) {
        return new Promise((resolve, reject) => {
            const record = {
                id: key,
                data: objectToProtoValueMap(data),
                vector: options.vector || [],
            };
            
            if (options.ttlSeconds) {
                record.ttl = Math.floor(Date.now() / 1000) + options.ttlSeconds;
            }
            
            const request = { key, record };
            
            this.client.Put(request, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(parseInt(response.version) || 0);
                }
            });
        });
    }
    
    /**
     * Delete a record
     * @param {string} key - The key to delete
     * @returns {Promise<boolean>}
     */
    async delete(key) {
        return new Promise((resolve, reject) => {
            this.client.Delete({ key }, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(response.success);
                }
            });
        });
    }
    
    /**
     * Scan records in a key range
     * @param {string} start - Start key
     * @param {string} end - End key
     * @param {number} limit - Maximum records
     * @returns {Promise<Record[]>}
     */
    async scan(start = '', end = '', limit = 100) {
        return new Promise((resolve, reject) => {
            const request = { start, end, limit };
            const records = [];
            
            const stream = this.client.Scan(request, this.metadata);
            
            stream.on('data', (protoRecord) => {
                records.push(Record.fromProto(protoRecord));
            });
            
            stream.on('end', () => resolve(records));
            stream.on('error', reject);
        });
    }
    
    /**
     * Batch store multiple records
     * @param {Object} entries - Dictionary of key -> data
     * @returns {Promise<number>} Count of records stored
     */
    async batchPut(entries) {
        return new Promise((resolve, reject) => {
            const protoEntries = {};
            
            for (const [key, data] of Object.entries(entries)) {
                protoEntries[key] = {
                    id: key,
                    data: objectToProtoValueMap(data),
                };
            }
            
            this.client.BatchPut({ entries: protoEntries }, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(response.count);
                }
            });
        });
    }
    
    // ==================== Vector Operations ====================
    
    /**
     * Add a vector to the index
     * @param {string} key - Unique identifier
     * @param {number[]} vector - The vector
     * @param {Object} metadata - Optional metadata
     * @returns {Promise<boolean>}
     */
    async vectorAdd(key, vector, metadata = {}) {
        return new Promise((resolve, reject) => {
            const request = {
                key,
                vector,
                metadata: objectToProtoValueMap(metadata),
            };
            
            this.client.VectorAdd(request, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(response.success);
                }
            });
        });
    }
    
    /**
     * Search for similar vectors
     * @param {number[]} query - Query vector
     * @param {number} k - Number of results
     * @returns {Promise<VectorResult[]>}
     */
    async vectorSearch(query, k = 10) {
        return new Promise((resolve, reject) => {
            this.client.VectorSearch({ query, k }, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    const results = response.results.map(r => new VectorResult({
                        id: r.id,
                        score: r.score,
                        record: Record.fromProto(r.record),
                    }));
                    resolve(results);
                }
            });
        });
    }
    
    // ==================== Pub/Sub ====================
    
    /**
     * Publish a message
     * @param {string} channel - Channel name
     * @param {Buffer} data - Message data
     * @returns {Promise<boolean>}
     */
    async publish(channel, data) {
        return new Promise((resolve, reject) => {
            this.client.Publish({ channel, data }, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(response.success);
                }
            });
        });
    }
    
    /**
     * Subscribe to a channel
     * @param {string} channel - Channel name
     * @param {string} subscriberId - Subscriber ID
     * @param {Function} onMessage - Callback for messages
     * @returns {EventEmitter}
     */
    subscribe(channel, subscriberId, onMessage) {
        const stream = this.client.Subscribe({ channel, subscriber_id: subscriberId }, this.metadata);
        
        stream.on('data', (message) => {
            onMessage({
                id: message.id,
                channel: message.channel,
                data: message.data,
                timestamp: new Date(message.timestamp * 1000),
            });
        });
        
        return stream;
    }
    
    // ==================== Query ====================
    
    /**
     * Execute a SQL-like query
     * @param {string} sql - SQL query
     * @returns {Promise<Record[]>}
     */
    async query(sql) {
        return new Promise((resolve, reject) => {
            this.client.Query({ query: sql }, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else if (!response.success) {
                    reject(new Error(response.error));
                } else {
                    resolve(response.records.map(r => Record.fromProto(r)));
                }
            });
        });
    }
    
    // ==================== Stats & Health ====================
    
    /**
     * Get database statistics
     * @returns {Promise<Object>}
     */
    async stats() {
        return new Promise((resolve, reject) => {
            this.client.Stats({}, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve({
                        recordsTotal: parseInt(response.records_total) || 0,
                        memoryUsed: parseInt(response.memory_used) || 0,
                        diskUsed: parseInt(response.disk_used) || 0,
                        cacheHitRatio: response.cache_hit_ratio,
                        avgQueryTimeNs: parseInt(response.avg_query_time_ns) || 0,
                        walSize: parseInt(response.wal_size) || 0,
                        mode: response.mode,
                        version: response.version,
                    });
                }
            });
        });
    }
    
    /**
     * Check server health
     * @returns {Promise<Object>}
     */
    async health() {
        return new Promise((resolve, reject) => {
            this.client.Health({}, this.metadata, (err, response) => {
                if (err) {
                    reject(err);
                } else {
                    resolve({
                        status: response.status,
                        timestamp: new Date(response.timestamp * 1000),
                        mode: response.mode,
                    });
                }
            });
        });
    }
}

// ==================== Helper Functions ====================

function objectToProtoValueMap(obj) {
    const result = {};
    
    for (const [key, value] of Object.entries(obj)) {
        result[key] = jsToProtoValue(value);
    }
    
    return result;
}

function jsToProtoValue(value) {
    const protoValue = {};
    
    if (value === null || value === undefined) {
        return protoValue;
    }
    
    if (typeof value === 'string') {
        protoValue.string_value = value;
    } else if (typeof value === 'number') {
        if (Number.isInteger(value)) {
            protoValue.int_value = value;
        } else {
            protoValue.float_value = value;
        }
    } else if (typeof value === 'boolean') {
        protoValue.bool_value = value;
    } else if (Buffer.isBuffer(value)) {
        protoValue.bytes_value = value;
    } else if (Array.isArray(value)) {
        protoValue.array_value = {
            values: value.map(v => jsToProtoValue(v)),
        };
    } else if (typeof value === 'object') {
        protoValue.map_value = {
            values: objectToProtoValueMap(value),
        };
    }
    
    return protoValue;
}

function protoValueMapToObject(map) {
    if (!map) return {};
    
    const result = {};
    
    for (const [key, value] of Object.entries(map)) {
        result[key] = protoValueToJs(value);
    }
    
    return result;
}

function protoValueToJs(value) {
    if (!value) return null;
    
    if (value.string_value !== undefined) {
        return value.string_value;
    } else if (value.int_value !== undefined) {
        return parseInt(value.int_value);
    } else if (value.float_value !== undefined) {
        return parseFloat(value.float_value);
    } else if (value.bool_value !== undefined) {
        return value.bool_value;
    } else if (value.bytes_value !== undefined) {
        return Buffer.from(value.bytes_value);
    } else if (value.array_value) {
        return value.array_value.values.map(v => protoValueToJs(v));
    } else if (value.map_value) {
        return protoValueMapToObject(value.map_value.values);
    }
    
    return null;
}

// ==================== Exports ====================

module.exports = {
    KviClient,
    Record,
    VectorResult,
    connect: (address, options) => new KviClient(address, options),
    version: '1.0.0',
};