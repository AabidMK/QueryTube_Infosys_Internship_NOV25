from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI
from .api.routes import router

app = FastAPI(title="YouTube AI Backend", version="1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # or restrict later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
