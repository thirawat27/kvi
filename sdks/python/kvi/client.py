import requests
from typing import Dict, Any, Optional, List

class KviClient:
    def __init__(self, host: str = "http://localhost:8080"):
        self.host = host.rstrip("/")
        self.base_url = f"{self.host}/api/v1"

    def put(self, key: str, data: Dict[str, Any]) -> bool:
        """Store a record by key"""
        url = f"{self.base_url}/put"
        payload = {"key": key, "data": data}
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.status_code == 201

    def get(self, key: str) -> Optional[Dict[str, Any]]:
        """Retrieve a record by key"""
        url = f"{self.base_url}/get"
        response = requests.get(url, params={"key": key})
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def query(self, sql_query: str) -> Any:
        """Execute a standard SQL query string"""
        url = f"{self.base_url}/query"
        payload = {"query": sql_query}
        response = requests.post(url, json=payload)
        response.raise_for_status()
        return response.json()

    def publish(self, channel: str, message: str) -> int:
        """Publish a message to a pub/sub channel"""
        url = f"{self.base_url}/pub"
        payload = {"channel": channel, "message": message}
        response = requests.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        return data.get("receivers", 0)

# Example Usage:
# if __name__ == "__main__":
#     client = KviClient()
#     client.put("user1", {"name": "Alice", "age": 25})
#     print(client.get("user1"))
#     print(client.query("SELECT * FROM users WHERE id = 'user1'"))
