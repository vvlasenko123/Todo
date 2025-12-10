from datetime import datetime
import asyncio
from typing import Set, List, Optional
import re
import httpx
from fastapi import FastAPI, HTTPException, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi import Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlmodel import SQLModel, Field

Base = declarative_base()
engine = create_async_engine(
    "sqlite+aiosqlite:///./tasks.db",
    echo=False,
)
DBSession = sessionmaker(
    bind=engine,
    autoflush=False,
    autocommit=False,
    class_=AsyncSession,
    expire_on_commit=False)


class TaskModel(SQLModel, table=True):
    __tablename__ = "tasks"

    id: int | None = Field(primary_key=True)
    title: str
    description: str
    done: bool = False

async def get_db():
    async with DBSession() as db:
        try:
            yield db
        finally:
            db.close()

app = FastAPI(title="TODO API", version="1.0")

class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, message: dict):
        if not self.active:
            return
        data = jsonable_encoder(message)
        remove = []
        for ws in list(self.active):
            try:
                await ws.send_json(data)
            except Exception:
                remove.append(ws)
        for ws in remove:
            self.disconnect(ws)

manager = ConnectionManager()

@app.on_event("startup")
async def startup():
    async with engine.begin() as conn:
        await conn.run_sync(
            SQLModel.metadata.create_all
        )

app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = datetime.now()
    response = await call_next(request)
    process_time = (datetime.now() - start_time).total_seconds()
    response.headers["X-Process-Time"] = f"{process_time:.4f}"
    print(f"Request to: {request.url.path} processed in {process_time:.4f} seconds")
    return response

@app.get("/add")
def add_numbers(a: int, b: int):
    return { "result": a + b }

class TaskCreate(BaseModel):
    title: str
    description: str

class Task(TaskCreate):
    id: int
    done: bool = False

class TaskUpdate(BaseModel):
    title: str
    description: str
    done: bool = False

@app.get("/tasks", response_model=list[TaskModel])
async def get_tasks(db: DBSession = Depends(get_db)):
    stmt = select(TaskModel)
    result = await db.execute(stmt)
    return result.scalars().all()

@app.get("/tasks/{task_id}", response_model=Task)
async def get_task(
        task_id: int,
        db: AsyncSession = Depends(get_db)
):
    obj = await db.get(TaskModel, task_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Task not found")
    return obj

@app.post("/tasks", response_model=Task, status_code=201)
async def create_task(
        task: TaskCreate,
        db: DBSession = Depends(get_db)
):
        new_task = TaskModel(
            title=task.title,
            description=task.description,
        )
        db.add(new_task)
        await db.commit()
        await db.refresh(new_task)

        await manager.broadcast(
            {
                "type": "created",
                "task": jsonable_encoder(new_task)
            }
        )
        return new_task

@app.put("/tasks/{task_id}", response_model=Task)
async def update_task(
        task_id: int,
        updated: TaskUpdate,
        db: DBSession = Depends(get_db)
):
    obj = await db.get(TaskModel, task_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Task not found")

    changed = False
    if updated.title is not None:
        obj.title = updated.title
        changed = True
    if updated.description is not None:
        obj.description = updated.description
        changed = True
    if updated.done is not None:
        obj.done = updated.done
        changed = True

    if changed:
        await db.commit()
        await db.refresh(obj)
        await manager.broadcast(
            {
                "type": "updated",
                "task": jsonable_encoder(obj)
            }
        )

    return obj

@app.patch("/tasks/{task_id}", response_model=Task)
async def patch_task(
    task_id: int,
    patch: TaskUpdate,
    db: DBSession = Depends(get_db)
):
    obj = await db.get(TaskModel, task_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Task not found")

    changed = False
    if patch.title is not None:
        obj.title = patch.title
        changed = True
    if patch.description is not None:
        obj.description = patch.description
        changed = True
    if patch.done is not None:
        obj.done = patch.done
        changed = True

    if changed:
        await db.commit()
        await db.refresh(obj)
        await manager.broadcast(
            {
                "type": "updated",
                "task": jsonable_encoder(obj)
            }
        )

    return obj


@app.delete("/tasks/{task_id}", status_code=204)
async def delete_task(
        task_id: int,
        db: DBSession = Depends(get_db)
):
    obj = await db.get(TaskModel, task_id)
    if not obj:
        raise HTTPException(status_code=404, detail="Task not found")
    await db.delete(obj)
    await db.commit()
    await manager.broadcast(
        {
            "type": "deleted",
            "task_id": task_id
        }
    )

@app.websocket("/ws/tasks")
async def websocket_tasks(ws: WebSocket):
    await manager.connect(ws)
    try:
        await ws.send_json({"type": "info", "message": "connected"})
        while True:
            try:
                data = await ws.receive_text()
            except WebSocketDisconnect:
                break
    except WebSocketDisconnect:
        pass
    finally:
        manager.disconnect(ws)

@app.get("/async_task")
async def async_task():
    await asyncio.sleep(60)
    return {"message": "ok"}

from fastapi import BackgroundTasks

@app.get("/background_task")
async def background_task(background_task: BackgroundTasks):
    def slow_time():
        import time
        time.sleep(60)

    background_task.add_task(slow_time)
    return {
        "message": "task started"
    }

from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

executor = ThreadPoolExecutor(max_workers=2)
executor = ProcessPoolExecutor(max_workers=2)

def blocking_io_task():
    import time
    time.sleep(60)
    return "ok"

@app.get("/thread_pool_sleep")
async def thread_pool_sleep():
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(executor, blocking_io_task)
    return { "message": result }

def heave_func(n: int = 10_000_000_000):
    result = 0
    for i in range(n):
        result += i * i
    return result

@app.get("/cpu_task")
async def cpu_task(n: int = 10_000_000_000):
    loop = asyncio.get_running_loop()
    result = await loop.run_in_executor(executor, heave_func, n)
    return {
        "message": heave_func(n)
    }

EXTERNAL_RANDOM_URL = "https://www.random.org/integers/?num=100&min=1&max=100&col=5&base=10&format=html&rnd=new"
POLL_INTERVAL_SECONDS = 10

_bg_task: asyncio.Task | None = None
_bg_task_cancelled = False
_bg_run_lock = asyncio.Lock()

async def run_once(source_url: str = EXTERNAL_RANDOM_URL):
    nums = await fetch_numbers_from_random_org(source_url)
    if not nums:
        return

    if _bg_run_lock.locked():
        print("Фоновая задача уже выполняется")
        return

    async with _bg_run_lock:
        inserted = await save_numbers_to_db(nums)
        print(f"Вставлено записей: {inserted}")

    try:
        await manager.broadcast(
            {
                "type": "bg_fill",
                "message": "Фоновые данные добавлены из random.org"
            }
        )
    except Exception as e:
        print(e)


async def fetch_numbers_from_random_org(
        source_url: str = EXTERNAL_RANDOM_URL,
        timeout: float = 15.0) -> Optional[List[str]]:
    try:
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                resp = await client.get(source_url)
            finally:
                pass
            if resp.status_code is not 200:
                return None
            text = resp.text
    finally:
        pass

    nums = re.findall(r"\d+", text)
    if not nums:
        return None
    return nums

async def save_numbers_to_db(numbers: List[str]) -> int:
    if not numbers:
        return 0

    inserted = 0
    async with DBSession() as db:
        try:
            for n in numbers:
                title = str(n)
                description = "random.org"
                new_task = TaskModel(
                    title=title,
                    description=description,
                    done=False
                )
                db.add(new_task)
                inserted += 1
            await db.commit()
        except Exception as e:
            await db.rollback()
            print(f"Ошибка записи в базу данных: {e}")
            return 0
    return inserted

async def background_fill_loop(interval_seconds: int = POLL_INTERVAL_SECONDS, source_url: str = EXTERNAL_RANDOM_URL):
    global _bg_task_cancelled
    while not _bg_task_cancelled:
        try:
            await run_once(source_url)
        finally:
            pass
        try:
            await asyncio.sleep(interval_seconds)
        finally:
            pass

@app.on_event("startup")
async def start_background_task():
    global _bg_task, _bg_task_cancelled
    if _bg_task is None or _bg_task.done():
        _bg_task_cancelled = False
        _bg_task = asyncio.create_task(background_fill_loop())

@app.on_event("shutdown")
async def stop_background_task():
    global _bg_task, _bg_task_cancelled
    _bg_task_cancelled = True
    if _bg_task is not None:
        _bg_task.cancel()
        try:
            await _bg_task
        finally:
            print("ok")
        _bg_task = None

@app.post("/task-generator/run")
async def run_task_generator_now():
    try:
        await run_once()
    except:
        raise HTTPException(
            status_code=500, detail=f"Ошибка выполнения фоновой задачи"
        )
    return {
        "message": "Фоновая задача выполнена"
    }

@app.post("/task-generator/stop")
async def stop_task_generator_now():
    global _bg_task, _bg_task_cancelled

    if _bg_task is None:
        return {
            "message": "Фоновая задача не запущена"
        }

    _bg_task_cancelled = True
    _bg_task.cancel()

    try:
        await _bg_task
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при остановке фоновой задачи: {e}")
    finally:
        _bg_task = None
        _bg_task_cancelled = False

    return {
        "message": "Фоновая задача остановлена"
    }
