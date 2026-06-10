from fastapi import FastAPI
from contextlib import asynccontextmanager
from app.config import settings
from app.services.cache import FeatureCache
from app.services.inference import InferenceEngine
from app.services.kafka_worker import MicrosecondPipelineWorker

state = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    cache = FeatureCache(redis_url=settings.REDIS_URL)
    engine = InferenceEngine(model_path=settings.MODEL_PATH)

    worker = MicrosecondPipelineWorker(
        bootstrap_servers=settings.KAFKA_BOOTSTRAP_SERVERS, cache=cache, engine=engine
    )

    await worker.start()
    state["worker"] = worker

    yield
    await state["worker"].stop()


app = FastAPI(title="FinGuard ML Service", lifespan=lifespan)


@app.get("/health", status_code=200)
async def health_check():
    return {"status": "HEALTHY", "engine": "ONNX_RUNTIME_ACTIVE"}
