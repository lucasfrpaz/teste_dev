import uvicorn
from fastapi import FastAPI
from router.rabbit import Rabbit
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from consumers import start_consumers, stop_consumers


_conn = None
_tasks = None
 
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _conn, _tasks
    _conn, _tasks = await start_consumers()
    try:
        yield
    finally:
        await stop_consumers(_conn, _tasks)

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

rabbit = Rabbit()
app.include_router(rabbit.router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=80, reload=True)
