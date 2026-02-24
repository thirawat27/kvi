class KviClient {
    /**
     * @param {string} host Base URL of the Kvi database server
     */
    constructor(host = "http://localhost:8080") {
        this.host = host.replace(/\/$/, "");
        this.baseUrl = `${this.host}/api/v1`;
    }

    /**
     * Store a record by key
     * @param {string} key Unique ID for the record
     * @param {object} data The object/data to store
     * @returns {Promise<boolean>} True if successful
     */
    async put(key, data) {
        const url = `${this.baseUrl}/put`;
        const payload = { key, data };

        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`KVi PUT Error: ${response.statusText}`);
        }
        return response.status === 201;
    }

    /**
     * Retrieve a record by key
     * @param {string} key Unique ID for the record
     * @returns {Promise<object|null>} The stored document/record or null if not found
     */
    async get(key) {
        const url = `${this.baseUrl}/get?key=${encodeURIComponent(key)}`;
        const response = await fetch(url, { method: "GET" });

        if (response.status === 404) return null;
        if (!response.ok) {
            throw new Error(`KVi GET Error: ${response.statusText}`);
        }
        return await response.json();
    }

    /**
     * Execute a standard SQL query string against KV engine
     * @param {string} sqlQuery Example: "SELECT * FROM users WHERE id = 'user1'"
     * @returns {Promise<object>} The result of the SQL execution
     */
    async query(sqlQuery) {
        const url = `${this.baseUrl}/query`;
        const payload = { query: sqlQuery };

        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`KVi SQL Query Error: ${await response.text()}`);
        }
        return await response.json();
    }

    /**
     * Publish a message to a pub/sub channel
     * @param {string} channel Notification channel name
     * @param {string} message The payload message
     * @returns {Promise<number>} Number of active subscribers that received the message
     */
    async publish(channel, message) {
        const url = `${this.baseUrl}/pub`;
        const payload = { channel, message };

        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        if (!response.ok) {
            throw new Error(`KVi Publish Error: ${await response.text()}`);
        }
        const data = await response.json();
        return data.receivers || 0;
    }
}

module.exports = KviClient;

// Example Usage:
// const client = new KviClient();
// client.query("SELECT * FROM users WHERE id = 'user1'").then(console.log);
