from pyArango.connection import Connection
from pyArango.connection import *

class ArangoConn():
    def __init__(self) -> None:
        conn = Connection(arangoURL="http://arango_db:8529", username="root", password="mypassword", verify=True ,verbose=True)
        self.db = conn.createDatabase(name="school")
        self.db = conn["school"]
    def test_connection(self) -> str:
        print(self.db)
    
        return {"database": "connected"}
