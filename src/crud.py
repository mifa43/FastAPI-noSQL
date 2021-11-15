from os import altsep
from pyArango.connection import Connection
from pyArango.connection import *
import csv
import pandas as pd
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
        return {"student": k['name'], "index": k['index-number'], "key": k['_key']}

    def update_student(self, name, index, key):
        update = self.db['Students']
        new=update[f"{key}"]
        new["name"] = name
        new["index-number"] = index
        new.save()
        print(new)
        return {"update": new}

    def list_student(self, args):
        list = self.db['Students']
        for student in list.fetchAll():
            if int(student["index-number"]) >= args:
                print(student["name"])
        return {"students": "lists of students"}

    def delete_student(self, key):
        student = self.db['Students']
        delete = student[f"{key}"]
        delete.delete()
        return {"delete": "deleted"}

    def querys_list(self):
        aql = "FOR x IN Students RETURN x._key"
        queryResult = self.db.AQLQuery(aql, rawResults = True, batchSize=100)
        for key in queryResult:
            print(key)
    
    def query_insert(self):
        docs = {"_key": "VinDizel", "name": "Vin", "index-number": 543}
        bind = {"docs": docs}
        aql = "INSERT @docs INTO Students LET newDoc = NEW RETURN newDoc"
        queryResult = self.db.AQLQuery(aql, bindVars=bind)
        print(queryResult)

    def add_csv(self):
        """updejtovanje citanjem csv fajla
        samo se prva row updejtuje 
        """
        with open("simple.csv", "r") as f:
            file = pd.read_csv(f)
            docs = {"index-number": file["index"][0]}
            bind = {"docs": docs, "key": file["_key"][0]}
            aql = "UPDATE @key WITH @docs IN Students LET updated = NEW RETURN updated"
            query = self.db.AQLQuery(aql, bindVars=bind)
            print(query)