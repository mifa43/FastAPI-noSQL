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

logger = logging.getLogger(__name__)
logger.setLevel("DEBUG")

app = FastAPI()

# @app.middleware('http')
# async def middleware_process(request: Request, call_next):
#     if request.headers["User-Agent"].find('Mobile') == -1:
#         print("PC user can use my api")
#         response = await call_next(request)
#         return response
#     else:
#         print("Phone user cant use my api")
#         return JSONResponse(content={
#             "message": "There is no phone response!"
#         }, status_code=401)

@app.get("/")
async def main():
    # connection = ArangoConn().test_connection()
    # collection = ArangoConn().create_collection()
    document = ArangoConn().create_documents()
    # print(connection['database'])
    # print(collection['newCollection'])
    print(document['newDocument'])

    return {"Hey": "John Doe"}

if __name__ == "__main__":
    uvicorn.run(app, port=8080, loop="asyncio")