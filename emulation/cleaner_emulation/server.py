from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
import uvicorn
import uuid
import sqlite3

app = FastAPI()

DB_FILE = "../boinc_server.db"
DEAD_STORAGE = "./dead_storage.db"


class WorkUnit(BaseModel):
    id: str
    data: str


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
            data TEXT
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
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS results (
            job_id TEXT PRIMARY KEY,
            client_id TEXT,
            result TEXT
        )
    """)
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS work_units_dead (
                id TEXT PRIMARY KEY,
                data TEXT
            )
        """)
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs_dead (
                id TEXT PRIMARY KEY,
                wu_id TEXT,
                assigned_to TEXT,
                status TEXT
            )
        """)
    cursor.execute("""
            CREATE TABLE IF NOT EXISTS results_dead (
                job_id TEXT PRIMARY KEY,
                client_id TEXT,
                result TEXT
            )
        """)
    conn.commit()
    conn.close()


init_db()


@app.post("/create_wu")
def create_work_unit(data: str):
    wu_id = str(uuid.uuid4())
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO work_units (id, data) VALUES (?, ?)", (wu_id, data)
    )
    job_id = str(uuid.uuid4())
    cursor.execute(
        "INSERT INTO jobs (id, wu_id, status) VALUES (?, ?, ?)", (job_id, wu_id, "pending")
    )
    conn.commit()
    conn.close()
    return {"wu_id": wu_id, "job_id": job_id}


@app.get("/get_job")
def get_job(client_id: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT id, wu_id FROM jobs WHERE status = 'pending' LIMIT 1")
    job = cursor.fetchone()
    if not job:
        return {"status": "no_jobs_available"}

    job_id, wu_id = job
    cursor.execute("SELECT data FROM work_units WHERE id = ?", (wu_id,))
    data = cursor.fetchone()[0]
    cursor.execute("UPDATE jobs SET assigned_to = ?, status = 'in_progress' WHERE id = ?", (client_id, job_id))
    conn.commit()
    conn.close()
    return {"job_id": job_id, "data": data}


@app.post("/submit_result")
def submit_result(result: Result):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("UPDATE jobs SET status = 'completed' WHERE id = ?", (result.job_id,))
    cursor.execute("INSERT INTO results (job_id, client_id, result) VALUES (?, ?, ?)",
                   (result.job_id, result.client_id, result.result))
    conn.commit()
    conn.close()
    return {"status": "success"}


def clean_work_units():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM work_units SELECT * FROM jobs WHERE status = 'success'")
    conn.commit()
    conn.close()


def clean_jobs():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM jobs WHERE status = 'success'")
    conn.commit()
    conn.close()


def clean_results():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM results SELECT * FROM work_units WHERE status = 'success'")
    conn.commit()
    conn.close()


def move_into_dead_storage_from_work_units():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO work_units_dead (id, data) 
        SELECT id, data FROM work_units
        WHERE status = 'success'
    """)
    conn.commit()
    conn.close()


def move_into_dead_storage_from_jobs():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
            INSERT INTO jobs_dead (id, data) 
            SELECT id, data FROM jobs
            WHERE status = 'success'
        """)
    conn.commit()
    conn.close()


def move_into_dead_storage_from_results():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("""
                INSERT INTO results_dead (id, data) 
                SELECT id, data FROM results
                WHERE status = 'success'
            """)
    conn.commit()
    conn.close()


@app.get("/stats")
def get_stats():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM work_units")
    total_wu = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'completed'")
    completed = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM jobs WHERE status = 'in_progress'")
    in_progress = cursor.fetchone()[0]
    conn.close()
    return {"total_work_units": total_wu, "completed": completed, "in_progress": in_progress}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
