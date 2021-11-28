from os import altsep
from pyArango.connection import Connection
from pyArango.connection import *
import csv
import pandas as pd
import time
import pyarrow.parquet as pq
import json
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

    def create_documents(self) -> str:
        time_start = time.time()
        file = pq.read_pandas("Industry").to_pandas()
        k = len(file['Year'])
        for i in range(k):
            document1 = self.db['Industry'].createDocument()
            document1["Year"] = file['Year'][i]
            document1["Industry_aggregation_NZSIOC"] = file['Industry_aggregation_NZSIOC'][i]
            document1["Industry_code_NZSIOC"] = file['Industry_code_NZSIOC'][i]
            document1["Industry_name_NZSIOC"] = file['Industry_name_NZSIOC'][i]
            document1["Units"] = file['Units'][i]
            document1["Variable_code"] = file['Variable_code'][i]
            document1["Variable_name"] = file['Variable_name'][i]
            document1["Variable_category"] = file['Variable_category'][i]
            document1["Value"] = file['Value'][i]
            document1["Industry_code_ANZSIC06"] = file['Industry_code_ANZSIC06'][i]
            document1.save()
        print((time.time() - time_start))

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


    def insert_parquet(self):
        time_start = time.time()
        file = pq.read_pandas("Industry").to_pandas()
        k = len(file['Year'])
        for i in range(k):
            
            docs = {
                "year": file['Year'][i],
                "industry_aggregation_NZSIOC": file['Industry_aggregation_NZSIOC'][i], 
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
  
    def bulk(self):
        with open("Industry.json", "r") as js:
            time_start = time.time()
            data = json.load(js)
            num = len(data["Year"])
            l = []
            for i in range(num):
                
                d = self.db['Industry'].createDocument()
                d["Year"] = data["Year"][f"{i}"]
                d["Industry_aggregation_NZSIOC"] = data["Industry_aggregation_NZSIOC"][f"{i}"]
                d["Industry_code_NZSIOC"] = data["Industry_code_NZSIOC"][f"{i}"]
                d["Industry_name_NZSIOC"] = data["Industry_name_NZSIOC"][f"{i}"]
                d["Units"] = data["Units"][f"{i}"]
                d["Variable_code"] = data["Variable_code"][f"{i}"]
                d["Variable_name"] = data["Variable_name"][f"{i}"]
                d["Variable_category"] = data["Variable_category"][f"{i}"]
                d["Value"] = data["Value"][f"{i}"]
                d["Industry_code_ANZSIC06"] = data["Industry_code_ANZSIC06"][f"{i}"]
                    
                l.append(d)
            c = self.db['Industry']
            c.bulkSave(l)
            print((time.time() - time_start))
    # def insert_csv(self):
    #     time_start = time.time()
    #     with open("annual-enterprise-survey-2020-financial-year-provisional-csv.csv", "r") as f:
    #         file = pd.read_csv(f)
    #         num = len(file['Year'])
    #         for i in range(num):
    #             #print(file['_key'][i], file['name'][i], file['index'][i])
    #             docs = {
    #                 "year": int(file['Year'][i]),
    #                 "industry_aggregation_NZSIOC": str(file['Industry_aggregation_NZSIOC'][i]), 
    #                 "industry_code_NZSIOC": file['Industry_code_NZSIOC'][i],
    #                 "industry_name_NZSIOC": file['Industry_name_NZSIOC'][i],
    #                 "units": file['Units'][i],
    #                 "variable_code": file['Variable_code'][i],
    #                 "variable_name": file['Variable_name'][i],
    #                 "variable_category": file['Variable_category'][i],
    #                 "value": file['Value'][i],
    #                 "industry_code_ANZSIC06": file['Industry_code_ANZSIC06'][i]
    #                 }
    #             bind = {"docs": docs}
    #             aql = "INSERT @docs INTO Industry LET newDoc = NEW RETURN newDoc"
    #             query = self.db.AQLQuery(aql, bindVars=bind)
    #             print(query)
    #     print((time.time() - time_start))
 

#year, industry_aggregation_NZSIOC, industry_code_NZSIOC, industry_name_NZSIOC, units, variable_code, variable_name, variable_category, value, industry_code_ANZSIC06, key
#76.6106607913971 - prvi test bez multiprocesa
#74.53744006156921 - test sa funkcijom
#58.07284164428711 - upisivanje bez aql-a 




# d["Year"] = data["Year"]
#                 d["Industry_aggregation_NZSIOC"] = data["Industry_aggregation_NZSIOC"]
#                 d["Industry_code_NZSIOC"] = data["Industry_code_NZSIOC"]
#                 d["Industry_name_NZSIOC"] = data["Industry_name_NZSIOC"]
#                 d["Units"] = data["Units"]
#                 d["Variable_code"] = data["Variable_code"]
#                 d["Variable_name"] = data["Variable_name"]
#                 d["Variable_category"] = data["Variable_category"]
#                 d["Value"] = data["Value"]
#                 d["Industry_code_ANZSIC06"] = data["Industry_code_ANZSIC06"]