from typing import Protocol
from arango import ArangoClient
from arango_orm import ConnectionPool

def connection():
    client = ArangoClient(
        hosts=['http://localhost:8529'],
        host_resolver='roundrobin'
        )
    #client1 = ArangoClient(protocol='http', host='localhost', port=8529)
    test_db = client.db('test', username = "root", password = "mypassword")
    db = ConnectionPool([client], "test", "root", "mypassword")
    return {"database": "connected"}