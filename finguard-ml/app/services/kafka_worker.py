import asyncio
import json
import logging
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError
from app.services.inference import InferenceEngine
from app.services.cache import FeatureCache

logger = logging.getLogger("finguard-worker")


class MicrosecondPipelineWorker:
    def __init__(
        self,
        cache: FeatureCache,
        engine: InferenceEngine,
        bootstrap_servers: str = None,
    ):
        self.cache = cache
        self.engine = engine
        self.bootstrap_servers = bootstrap_servers
        self.consumer = None
        self.producer = None
        self.task = None
        self.is_running = False

    async def start(self):
        logger.info("Initializing event streams...")

        self.consumer = AIOKafkaConsumer(
            "transactions.raw",
            bootstrap_servers=self.bootstrap_servers,
            group_id="finguard-ml-matrix",
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers
        )

        while True:
            try:
                await self.consumer.start()
                await self.producer.start()
                logger.info("Live stream connection established with Kafka brokers.")
                break
            except KafkaConnectionError:
                logger.warning(
                    "Kafka brokers not accessible yet. Re-attempting connection in 3 seconds..."
                )
                await asyncio.sleep(3)

        self.is_running = True
        self.task = asyncio.create_task(self._loop())

    async def _loop(self):
        try:
            async for msg in self.consumer:
                if not self.is_running:
                    break

                payload = json.loads(msg.value.decode("utf-8"))
                account_id = payload.get("account_id")

                if not account_id:
                    logger.warning(
                        "Dropped event packet: missing critical 'account_id' field."
                    )
                    continue

                profile = await self.cache.get_user_profile(account_id)

                features = [
                    float(payload.get("amount", 0.0)),
                    float(profile.get("historical_risk_score", 0.05)),
                    float(profile.get("velocity_1h", 0)),
                ]

                try:
                    fraud_score = self.engine.compute_fraud_score(features)
                except Exception as ex:
                    logger.error(
                        f"Inference failure on account {account_id}: {str(ex)}"
                    )
                    fraud_score = 0.5

                payload["fraud_score"] = round(fraud_score, 4)

                logger.info(
                    "Scored account=%s amount=%s fraud_score=%.4f",
                    account_id, payload.get("amount"), fraud_score,
                )

                await self.producer.send_and_wait(
                    "transactions.evaluated",
                    value=json.dumps(payload).encode("utf-8"),
                    key=account_id.encode("utf-8"),
                )
        except asyncio.CancelledError:
            logger.info("Kafka pipeline consumer worker shutting down gracefully.")
        except Exception as e:
            logger.error(f"Critical error in execution loop: {str(e)}")

    async def stop(self):
        if not self.is_running:
            return

        logger.info("Draining outbox queues and stopping broker consumers...")
        self.is_running = False

        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass

        if self.consumer:
            await self.consumer.stop()
        if self.producer:
            await self.producer.stop()

        logger.info("Pipeline worker resource teardown complete.")
