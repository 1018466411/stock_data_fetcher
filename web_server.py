import os
import subprocess
import threading
from fastapi import FastAPI, Request, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime
import db

app = FastAPI(title="Stock Data Fetcher Manager")

# Templates setup
templates_dir = os.path.join(os.path.dirname(__file__), "templates")
if not os.path.exists(templates_dir):
    os.makedirs(templates_dir)
templates = Jinja2Templates(directory=templates_dir)

# Process tracking
running_tasks: Dict[str, subprocess.Popen] = {}
task_logs: Dict[str, List[str]] = {}
task_lock = threading.Lock()

class SqlQueryRequest(BaseModel):
    query: str

class ApiKeyRequest(BaseModel):
    api_key: str

def run_script_in_background(task_id: str, cmd: List[str]):
    try:
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            cwd=os.path.dirname(__file__)
        )
        with task_lock:
            running_tasks[task_id] = process
            task_logs[task_id] = []

        for line in process.stdout:
            with task_lock:
                task_logs[task_id].append(line.strip())
                if len(task_logs[task_id]) > 1000:
                    task_logs[task_id].pop(0)
                    
        process.wait()
    except Exception as e:
        with task_lock:
            if task_id not in task_logs:
                task_logs[task_id] = []
            task_logs[task_id].append(f"Error: {e}")
    finally:
        with task_lock:
            if task_id in running_tasks:
                del running_tasks[task_id]

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/data", response_class=HTMLResponse)
async def data_viewer(request: Request):
    return templates.TemplateResponse("data.html", {"request": request})

@app.get("/config", response_class=HTMLResponse)
async def config_viewer(request: Request):
    return templates.TemplateResponse("config.html", {"request": request})

@app.get("/api/tasks")
async def get_tasks():
    with task_lock:
        active = []
        for tid, proc in running_tasks.items():
            active.append({
                "id": tid,
                "pid": proc.pid,
                "status": "running" if proc.poll() is None else "stopped"
            })
        return {"tasks": active}

@app.get("/api/config/apikey")
async def get_api_key():
    try:
        api_key = db.get_api_key()
        if not api_key:
            config = db.get_config()
            api_key = config.get("api", {}).get("api_key", "")
        return {"api_key": api_key}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/config/apikey")
async def update_api_key(req: ApiKeyRequest):
    try:
        # 先验证 API Key
        config = db.get_config()
        api_domain = config.get("api", {}).get("domain", "https://data.diemeng.chat").rstrip('/')
        
        import requests
        test_url = f"{api_domain}/api/stock/list"
        headers = {"apiKey": req.api_key}
        
        response = requests.get(test_url, headers=headers, timeout=10)
        
        try:
            data = response.json()
            if response.status_code != 200 or data.get('code') != 200:
                error_msg = data.get('msg') if isinstance(data, dict) else 'Unknown error'
                return JSONResponse(status_code=400, content={"error": f"API Key 无效或未开通权限: {error_msg}"})
        except ValueError:
            return JSONResponse(status_code=400, content={"error": "API Key 验证失败：服务端返回非 JSON 数据"})
            
        # 验证通过，保存到 .env 文件
        db.set_api_key(req.api_key)
            
        return {"message": "API Key updated successfully"}
    except requests.exceptions.RequestException as e:
        return JSONResponse(status_code=400, content={"error": f"验证网络请求失败，请重试: {str(e)}"})
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/api/tasks/{task_id}/logs")
async def get_task_logs(task_id: str):
    with task_lock:
        logs = task_logs.get(task_id, [])
        return {"logs": logs}

@app.post("/api/tasks/start/history")
async def start_history_task(
    request: Request,
    background_tasks: BackgroundTasks,
    levels: str = Form("1m,daily"),
    start_time: str = Form("2023-01-01 00:00:00"),
    end_time: str = Form("")
):
    task_id = "history"
    with task_lock:
        if task_id in running_tasks and running_tasks[task_id].poll() is None:
            return JSONResponse(status_code=400, content={"error": "历史数据获取任务已在运行中"})

    # 优先从请求参数中获取 rate_limit，如果没有则默认 300
    query_rate_limit = request.query_params.get("rate_limit")
    if query_rate_limit and query_rate_limit.isdigit():
        rate_limit = int(query_rate_limit)
    else:
        rate_limit = 300

    cmd = ["python", "fetch_history.py", "--levels", levels, "--rate_limit", str(rate_limit)]
    if start_time:
        cmd.extend(["--start_time", start_time])
    if end_time:
        cmd.extend(["--end_time", end_time])
        
    background_tasks.add_task(run_script_in_background, task_id, cmd)
    return {"message": "Task started", "task_id": task_id, "applied_rate_limit": rate_limit}

@app.post("/api/tasks/start/realtime")
async def start_realtime_task(background_tasks: BackgroundTasks):
    task_id = "realtime"
    with task_lock:
        if task_id in running_tasks and running_tasks[task_id].poll() is None:
            return JSONResponse(status_code=400, content={"error": "实时快照任务已在运行中"})
            
    cmd = ["python", "fetch_realtime.py"]
    background_tasks.add_task(run_script_in_background, task_id, cmd)
    return {"message": "Task started", "task_id": task_id}

@app.post("/api/tasks/start/realtime_minute")
async def start_realtime_minute_task(background_tasks: BackgroundTasks):
    task_id = "realtime_minute"
    with task_lock:
        if task_id in running_tasks and running_tasks[task_id].poll() is None:
            return JSONResponse(status_code=400, content={"error": "实时分时及补缺任务已在运行中"})
            
    cmd = ["python", "fetch_realtime_minute.py"]
    background_tasks.add_task(run_script_in_background, task_id, cmd)
    return {"message": "Task started", "task_id": task_id}

@app.post("/api/tasks/start/ws")
async def start_ws_task(background_tasks: BackgroundTasks):
    task_id = "ws"
    with task_lock:
        if task_id in running_tasks and running_tasks[task_id].poll() is None:
            return JSONResponse(status_code=400, content={"error": "WebSocket 任务已在运行中"})
            
    cmd = ["python", "fetch_ws.py"]
    background_tasks.add_task(run_script_in_background, task_id, cmd)
    return {"message": "Task started", "task_id": task_id}

@app.post("/api/tasks/start/limit_status")
async def start_limit_status_task(background_tasks: BackgroundTasks):
    return JSONResponse(
        status_code=400,
        content={
            "error": "已合并到 fetch_realtime.py，请使用 /api/tasks/start/realtime 启动，避免重复轮询超限。"
        },
    )

@app.post("/api/tasks/stop/{task_id}")
async def stop_task(task_id: str):
    with task_lock:
        proc = running_tasks.get(task_id)
        if proc:
            proc.terminate()
            return {"message": "Task terminated"}
        return {"message": "Task not found"}, 404

@app.post("/api/query")
async def execute_query(req: SqlQueryRequest):
    try:
        from db import get_db
        db_instance = get_db()
        query = req.query.strip()
        # 如果用户没有写 LIMIT，自动加上 LIMIT 1000 以防止拖垮浏览器和服务器
        if "LIMIT " not in query.upper() and not query.endswith(";"):
            query = f"{query} LIMIT 1000"
        elif "LIMIT " not in query.upper() and query.endswith(";"):
            query = f"{query[:-1]} LIMIT 1000"
            
        result, columns = db_instance.query(query, with_column_types=True)
        col_names = [c[0] for c in columns]
        
        # Format output
        data = []
        for row in result:
            row_dict = {}
            for i, val in enumerate(row):
                if isinstance(val, datetime):
                    row_dict[col_names[i]] = val.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    row_dict[col_names[i]] = val
            data.append(row_dict)
            
        return {"columns": col_names, "data": data}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

@app.get("/api/tables")
async def get_tables():
    try:
        from db import get_db
        db_instance = get_db()
        result = db_instance.query("SHOW TABLES")
        tables = [r[0] for r in result]
        return {"tables": tables}
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

class TableConfigRequest(BaseModel):
    tables: Dict[str, Any]

@app.get("/api/config/tables")
async def get_table_config():
    try:
        config = db.get_config()
        tables = config.get("tables", {})
        # Merge with default schema to provide all available keys
        from db import DEFAULT_SCHEMA
        result = {}
        for key, schema in DEFAULT_SCHEMA.items():
            conf = tables.get(key, {})
            result[key] = {
                "name": conf.get("name", f"stock_{key}"),
                "fields": {k: conf.get("fields", {}).get(k, k) for k in schema["fields"].keys()}
            }
        return {"tables": result}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.post("/api/config/tables")
async def update_table_config(req: TableConfigRequest):
    try:
        config = db.get_config()
        old_tables = config.get("tables", {})
        new_tables = req.tables
        
        from db import get_db
        db_instance = get_db()
        
        for key, new_conf in new_tables.items():
            old_conf = old_tables.get(key, {})
            if old_conf:
                # Perform ALTER TABLE
                db_instance.alter_table(key, old_conf, new_conf)
                
        config["tables"] = new_tables
        db.save_config(config)
        # re-init DB to apply new configs in memory
        db_instance.config = config
        db_instance.tables_config = new_tables
        
        return {"message": "Table configuration updated successfully"}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)
