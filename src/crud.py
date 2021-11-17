from os import altsep
from pyArango.connection import Connection
from pyArango.connection import *
import csv
import pandas as pd
import time

class ArangoConn():
    def __init__(self) -> None:
        conn = Connection(arangoURL="http://arango_db:8529", username="root", password="mypassword", verify=True ,verbose=True)
        #self.db = conn.createDatabase(name="school")
        self.db = conn["school"]
    def test_connection(self) -> str:
        print(self.db)
    
        return {"database": "connected"}
    def create_collection(self) -> str:
        self.createNewCollection = self.db.createCollection(name="Industry")
        return {"newCollection": self.db["Industry"]}

    def create_documents(self, year, industry_aggregation_NZSIOC, industry_code_NZSIOC, industry_name_NZSIOC, 
    units, variable_code, variable_name, variable_category, value, industry_code_ANZSIC06, key) -> str:
        document1 = self.db['Industry'].createDocument()
        document1["Year"] = f"{year}"
        document1["Industry_aggregation_NZSIOC"] = f"{industry_aggregation_NZSIOC}"
        document1["Industry_code_NZSIOC"] = f"{industry_code_NZSIOC}"
        document1["Industry_name_NZSIOC"] = f"{industry_name_NZSIOC}"
        document1["Units"] = f"{units}"
        document1["Variable_code"] = f"{variable_code}"
        document1["Variable_name"] = f"{variable_name}"
        document1["Variable_category"] = f"{variable_category}"
        document1["Value"] = f"{value}"
        document1["Industry_code_ANZSIC06"] = f"{industry_code_ANZSIC06}"
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


    def insert_csv(self):
        time_start = time.time()
        with open("annual-enterprise-survey-2020-financial-year-provisional-csv.csv", "r") as f:
            file = pd.read_csv(f)
            num = len(file['Year'])
            for i in range(num):
                #print(file['_key'][i], file['name'][i], file['index'][i])
                docs = {
                    "year": int(file['Year'][i]),
                    "industry_aggregation_NZSIOC": str(file['Industry_aggregation_NZSIOC'][i]), 
                    "industry_code_NZSIOC": file['Industry_code_NZSIOC'][i],
                    "industry_name_NZSIOC": file['Industry_name_NZSIOC'][i],
                    "units": file['Units'][i],
                    "variable_code": file['Variable_code'][i],
                    "variable_name": file['Variable_name'][i],
                    "variable_category": file['Variable_category'][i],
                    "value": file['Value'][i],
                    "industry_code_ANZSIC06": file['Industry_code_ANZSIC06'][i]
                    }
                bind = {"docs": docs}
                aql = "INSERT @docs INTO Industry LET newDoc = NEW RETURN newDoc"
                query = self.db.AQLQuery(aql, bindVars=bind)
                print(query)
        print((time.time() - time_start))
 

#year, industry_aggregation_NZSIOC, industry_code_NZSIOC, industry_name_NZSIOC, units, variable_code, variable_name, variable_category, value, industry_code_ANZSIC06, key
#76.6106607913971 - prvi test bez multiprocesa
#74.53744006156921 - test sa funkcijom