from neo4j import GraphDatabase
from threading import Lock

class Neo4jFactory:
    _instance = None
    _lock = Lock()
    _driver = None

    def __new__(cls, uri=None, user=None, password=None):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super(Neo4jFactory, cls).__new__(cls)
                    if uri and user and password:
                        cls._driver = GraphDatabase.driver(uri, auth=(user, password))
        return cls._instance

    def get_driver(self):
        if not self._driver:
            raise Exception("Neo4j driver not initialized. Please provide uri, user, and password.")
        return self._driver

    @classmethod
    def close(cls):
        if cls._driver:
            cls._driver.close()
            cls._driver