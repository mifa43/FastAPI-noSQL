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
logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

class StudentModel(BaseModel):
    name: Optional[str] = None
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


@app.put("/update-student")
def update_student(model: StudentModel):
    update = ArangoConn().update_student(model.name, model.key)
    update['update']
    return {"message": "update"}
@app.post("/get-student")
async def get_student(model: StudentModel):
    student = ArangoConn().get_student(model.key)
    print(student)
    return {"message": student}

@app.post("/add-student")
async def add_student(model: StudentModel):
    document = ArangoConn().create_documents(model.name, model.key)
    print(document['newDocument'])
    return {"message": "New student"}

@app.get("/health_check")
async def health_check():

    # connection = ArangoConn().test_connection()
    # collection = ArangoConn().create_collection()
    #document = ArangoConn().create_documents()
    # print(connection['database'])
    # print(collection['newCollection'])
    #print(document['newDocument'])
  

    return {"Health": "OK"}

if __name__ == "__main__":
    uvicorn.run(app, port=8080, loop="asyncio")