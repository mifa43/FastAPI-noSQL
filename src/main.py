import logging
import uvicorn
from fastapi import FastAPI, Request, HTTPException, status
import time
from fastapi.responses import JSONResponse
import pyarrow.parquet as pq
import numpy as np
import pandas as pd
import pyarrow as pa
import csv
from crud import ArangoConn
from pydantic import BaseModel
from typing import Optional
import time
import multiprocessing as mp
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

class StudentModel(BaseModel):
    name: Optional[str] = None
    index: Optional[int] = None
    key: Optional[str] = None

app = FastAPI()



@app.middleware('http')
async def middleware_process(request: Request, call_next):
    if request.headers["User-Agent"].find('Mobile') == -1:
        print("PC user can use my api")
        response = await call_next(request)
        return response
    else:
        print("Phone user cant use my api")
        return JSONResponse(content={
            "message": "There is no phone response!"
        }, status_code=401)

@app.delete("/delete-student")
async def delete_student(model: StudentModel):
    delete = ArangoConn().delete_student(model.key)
    print(delete['delete'])
    return {"message": "student deleted"}

@app.get("/list-students")
async def get_students():
    ls =  ArangoConn().list_student(1)
    print(ls['students'])
    return {"message": "list of students"}

@app.put("/update-student")
def update_student(model: StudentModel):
    update = ArangoConn().update_student(model.name, model.index ,model.key)
    update['update']
    return {"message": "update"}

@app.post("/get-student")
async def get_student(model: StudentModel):
    student = ArangoConn().get_student(model.key)
    print(student)
    return {"message": student}

@app.post("/add-student")
async def add_student(model: StudentModel):
    document = ArangoConn().create_documents(model.name, model.index ,model.key)
    print(document['newDocument'])
    return {"message": "New student"}

@app.get("/health_check")
async def health_check():
    ArangoConn().bulk()
    # s = ArangoConn().insert_parquet()
    # print(s)
    # aql1 = ArangoConn().query_insert()
    # print(aql1)
    # aql2 = ArangoConn().add_csv()
    # print(aql2)
    # aql = ArangoConn().querys_list()
    # print(aql)
    
    # connection = ArangoConn().test_connection()
    # collection = ArangoConn().create_collection()
    #document = ArangoConn().create_documents()
    # print(connection['database'])
    # print(collection['newCollection'])
    #print(document['newDocument'])
  
    return {"Health": "OK"}

if __name__ == "__main__":
    uvicorn.run(app, port=8080, loop="asyncio")