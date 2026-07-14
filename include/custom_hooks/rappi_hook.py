from airflow.hooks.base import BaseHook
from typing import Optional
import requests


class RappiApiHook(BaseHook):
    """
    Custom hook for interacting with a simple API.
    
    This hook demonstrates how to create a custom hook that:
    - Connects to an external service
    - Has custom methods for specific operations
    - Uses Airflow connections for credential management
    """
    
    conn_name_attr = 'api_conn_id'
    default_conn_name = 'simple_api_default'
    conn_type = 'http'
    hook_name = 'Simple API'
    
    def __init__(
        self,
        api_conn_id: str = default_conn_name,
        timeout: int = 30,
        **kwargs
    ):
        super().__init__()
        self.api_conn_id = api_conn_id
        self.timeout = timeout
        self.base_url = None
        self.session = None
        
        # Log initialization
        self.log.info(f"Initializing SimpleApiHook with connection: {api_conn_id}")
    
    def get_conn(self):
        """
        Get connection and return a requests session.
        This method is called to establish the connection.
        """
        if self.session is not None:
            return self.session
        
        # Get connection details from Airflow
        conn = self.get_connection(self.api_conn_id)
        
        # Build base URL
        schema = conn.schema or 'https'
        host = conn.host
        port = f":{conn.port}" if conn.port else ""
        self.base_url = f"{schema}://{host}{port}"
        
        # Create session
        self.session = requests.Session()
        
        # Add authentication if provided
        if conn.login and conn.password:
            self.session.auth = (conn.login, conn.password)
        
        # Add headers if provided in extra
        if conn.extra_dejson:
            headers = conn.extra_dejson.get('headers', {})
            self.session.headers.update(headers)
        
        self.log.info(f"Connection established to: {self.base_url}")
        
        return self.session
    
    def test_connection(self):
        """
        Test the connection to the API.
        This is called when you test the connection in Airflow UI.
        """
        try:
            session = self.get_conn()
            response = session.get(f"{self.base_url}/health", timeout=self.timeout)
            response.raise_for_status()
            
            self.log.info("Connection test successful! ✅")
            return True, "Connection successfully tested"
            
        except Exception as e:
            self.log.error(f"Connection test failed! ❌")
            return False, str(e)
    
    def get_data(self, endpoint: str, params: Optional[dict] = None):
        """
        GET request to fetch data from API.
        
        Args:
            endpoint: API endpoint (without base URL)
            params: Query parameters
        
        Returns:
            JSON response from API
        """
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        self.log.info(f"GET request to: {url}")
        self.log.info(f"Parameters: {params}")
        
        try:
            response = session.get(url, params=params, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            self.log.info(f"Successfully fetched data: {len(str(data))} bytes")
            
            return data
            
        except requests.exceptions.RequestException as e:
            self.log.error(f"API request failed: {str(e)}")
            raise
    
    def post_data(self, endpoint: str, data: dict):
        """
        POST request to send data to API.
        
        Args:
            endpoint: API endpoint (without base URL)
            data: Data to send in request body
        
        Returns:
            JSON response from API
        """
        session = self.get_conn()
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        self.log.info(f"POST request to: {url}")
        self.log.info(f"Payload size: {len(str(data))} bytes")
        
        try:
            response = session.post(url, json=data, timeout=self.timeout)
            response.raise_for_status()
            
            result = response.json()
            self.log.info(f"Successfully posted data. Response: {result}")
            
            return result
            
        except requests.exceptions.RequestException as e:
            self.log.error(f"API request failed: {str(e)}")
            raise
    
    def close(self):
        """Close the connection."""
        if self.session:
            self.session.close()
            self.log.info("Connection closed")


class RappiFileHook(BaseHook):
    """
    Alternative simple hook for file operations.
    Demonstrates a different type of custom hook.
    """
    
    conn_name_attr = 'file_conn_id'
    default_conn_name = 'file_default'
    conn_type = 'fs'
    hook_name = 'Simple File'
    
    def __init__(self, file_conn_id: str = default_conn_name):
        super().__init__()
        self.file_conn_id = file_conn_id
        self.base_path = None
        
        self.log.info(f"Initializing SimpleFileHook")
    
    def get_conn(self):
        """Get the file system connection."""
        conn = self.get_connection(self.file_conn_id)
        self.base_path = conn.host or "/tmp"
        
        self.log.info(f"File connection established. Base path: {self.base_path}")
        return self.base_path
    
    def read_file(self, filename: str) -> str:
        """Read a file."""
        base_path = self.get_conn()
        full_path = f"{base_path}/{filename}"
        
        self.log.info(f"Reading file: {full_path}")
        
        try:
            with open(full_path, 'r') as f:
                content = f.read()
            
            self.log.info(f"Successfully read {len(content)} bytes")
            return content
            
        except Exception as e:
            self.log.error(f"Failed to read file: {str(e)}")
            raise
    
    def write_file(self, filename: str, content: str):
        """Write to a file."""
        base_path = self.get_conn()
        full_path = f"{base_path}/{filename}"
        
        self.log.info(f"Writing to file: {full_path}")
        
        try:
            with open(full_path, 'w') as f:
                f.write(content)
            
            self.log.info(f"Successfully wrote {len(content)} bytes")
            
        except Exception as e:
            self.log.error(f"Failed to write file: {str(e)}")
            raise