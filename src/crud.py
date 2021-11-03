from typing import Protocol
from arango import ArangoClient
from arango_orm import ConnectionPool

class ArangoConn():
    def __init__(self, *db) -> None:
        self.db = db
    def test_connection(self):
        client = ArangoClient(
            hosts=['http://localhost:8529'],
            host_resolver='roundrobin'
            )
        #client1 = ArangoClient(protocol='http', host='localhost', port=8529)
        test_db = client.db('test', username = "root", password = "mypassword")
        self.db = ConnectionPool([client], "test", "root", "mypassword")
        return {"database": "connected"}
    def create_collections(self):
        pass
