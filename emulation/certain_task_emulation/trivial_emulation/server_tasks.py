from fastapi import FastAPI
from pydantic import BaseModel
import uuid
import sqlite3
import uvicorn


app = FastAPI()
DB_FILE = "../../boinc_server.db"


class WorkUnit(BaseModel):
    id: str
    data: str
    func_code: str


class Job(BaseModel):
    id: str
    wu_id: str
    assigned_to: str = None
    status: str = "pending"


class Result(BaseModel):
    job_id: str
    client_id: str
    result: str


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS work_units (
            id TEXT PRIMARY KEY,
            data TEXT,
            func_code TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            wu_id TEXT,
            assigned_to TEXT,
            status TEXT
        )
    """)
    conn.commit()
    conn.close()


init_db()


@app.post("/create_wu")
def create_work_unit(data: str, func_code: str):
    wu_id = str(uuid.uuid4())
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO work_units (id, data, func_code) VALUES (?, ?, ?)",
        (wu_id, data, func_code)
    )
    job_id = str(uuid.uuid4())
    cursor.execute(
        "INSERT INTO jobs (id, wu_id, status) VALUES (?, ?, ?)",
        (job_id, wu_id, "pending")
    )
    conn.commit()
    conn.close()
    return {"wu_id": wu_id, "job_id": job_id}


@app.get("/get_job")
def get_job(client_id: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT jobs.id, work_units.data, work_units.func_code 
        FROM jobs 
        JOIN work_units ON jobs.wu_id = work_units.id 
        WHERE jobs.status = 'pending' 
        LIMIT 1
    """)
    job = cursor.fetchone()
    if not job:
        return {"status": "no_jobs_available"}

    job_id, data, func_code = job
    cursor.execute(
        "UPDATE jobs SET assigned_to = ?, status = 'in_progress' WHERE id = ?",
        (client_id, job_id)
    )
    conn.commit()
    conn.close()
    return {"job_id": job_id, "data": data, "func_code": func_code}


@app.post("/submit_result")
def submit_result(result: Result):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE jobs SET status = 'completed' WHERE id = ?",
        (result.job_id,)
    )
    cursor.execute(
        "INSERT INTO results (job_id, client_id, result) VALUES (?, ?, ?)",
        (result.job_id, result.client_id, result.result)
    )
    conn.commit()
    conn.close()
    return {"status": "success"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)