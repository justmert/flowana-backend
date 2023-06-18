from datetime import datetime
import json
from typing import Union
from fastapi import FastAPI

app = FastAPI()


@app.get("/")
def read_root():
    return "Hello World!"

@app.get("/commit_history")
def commit_history():
    chart = {
        "title": {
            "text": "ECharts Getting Started Example"
        },
        "tooltip": {},
        "xAxis": {
            "data": ['shirt', 'cardigan', 'chiffon', 'pants', 'heels', 'socks']
        },
        "yAxis": {},
        "series": [
            {
                "name": 'sales',
                "type": 'bar',
                "data": [5, 20, 36, 10, 10, 20]
            }
        ]
    }
    return chart

