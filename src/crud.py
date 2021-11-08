from pyArango.connection import Connection
from pyArango.connection import *

class ArangoConn():
    def __init__(self) -> None:
        conn = Connection(arangoURL="http://arango_db:8529", username="root", password="mypassword", verify=True ,verbose=True)
        #self.db = conn.createDatabase(name="school")
        self.db = conn["school"]
    def test_connection(self) -> str:
        print(self.db)
    
        return {"database": "connected"}
    def create_collection(self) -> str:
        self.createNewCollection = self.db.createCollection(name="Students")
        return {"newCollection": self.db["Students"]}

    def create_documents(self, name, index_num, key) -> str:
        document1 = self.db['Students'].createDocument()
        document1["name"] = f"{name}"
        document1["index-number"] = f"{index_num}"
        document1._key = f"{key}"
        document1.save()
        return {"newDocument": document1}
    
    def get_student(self, key):
        document1 = self.db['Students']
        k=document1[f"{key}"]
        return {"student": k['name'], "key": k['_key']}

    def update_student(self, name, key):
        update = self.db['Students']
        new=update[f"{key}"]
        new["name"] = name
        new.save()
        print(new)
        return {"update": new}

    def list_student(self, args):
        list = self.db['Students']
        for student in list.fetchAll():
            if int(student["index-number"]) >= args:
                print(student["name"])
                

        return {"students": "lists of students"}