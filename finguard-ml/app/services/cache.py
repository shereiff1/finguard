import redis.asyncio as aioredis
import json


class FeatureCache:
    def __init__(self, redis_url: str):
        self.pool = aioredis.ConnectionPool.from_url(redis_url, decode_responses=True)

    async def get_user_profile(self, account_id: str) -> dict:
        async with aioredis.Redis(connection_pool=self.pool) as client:
            profile_data = await client.get(f"profile:{account_id}")
            if profile_data:
                return json.loads(profile_data)

            return {"historical_risk_score": 0.05, "velocity_1h": 0}
