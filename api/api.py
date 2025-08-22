from fastapi import FastAPI
from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import pandas as pd
from datetime import datetime, timedelta
from fastapi_cache import FastAPICache
from fastapi_cache.decorator import cache
from fastapi_cache.backends.inmemory import InMemoryBackend
from queries import MINT_BURN_QUERIES
import os
app = FastAPI()

# Initialize cache on startup
@app.on_event("startup")
async def startup():
    FastAPICache.init(InMemoryBackend())

# Constants and client setup
PRECISION = 1e9
GRAPHQL_URL = os.getenv('GRAPHQL_URL', 'http://localhost:8080/v1/graphql')
CACHE_TTL = 4*60*60

transport = RequestsHTTPTransport(url=GRAPHQL_URL)
client = Client(transport=transport, fetch_schema_from_transport=False)

@app.get("/distribution")
@cache(expire=CACHE_TTL)  # Cache for 4 hours
async def get_distribution():
    try:
        two_weeks_ago = datetime.now() - timedelta(days=14)
        mint_result = client.execute(gql(MINT_BURN_QUERIES["mint"]))
        mint_df = pd.DataFrame(mint_result['USDM_Mint'])
        mint_df['timestamp'] = pd.to_datetime(mint_df['timestamp'], unit='s')
        mint_df['amount'] = mint_df['amount'].astype(float) / PRECISION
        
        two_weeks_mints = mint_df[mint_df['timestamp'] >= two_weeks_ago]['amount'].sum()
        two_week_distribution = two_weeks_mints / 200

        return {
            "two_week_distribution": two_week_distribution,
            "total_mints": two_weeks_mints,
            "timestamp": datetime.now().isoformat(),
            "cache_timestamp": datetime.now().isoformat(),
            "cache_ttl": CACHE_TTL
        }
    except Exception as e:
        return {"error": str(e)}, 500

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
