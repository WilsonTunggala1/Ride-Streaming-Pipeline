from fastapi import FastAPI
import redis

app = FastAPI(title="Ride-Hailing Demand API")

# Connect to Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

@app.get("/")
def health_check():
    return {"status": "healthy", "service": "demand-api"}

@app.get("/demand/{h3_cell}")
def get_single_cell_demand(h3_cell: str):
    """Fetch the current demand for a specific H3 hexagon."""
    demand = r.get(f"demand:{h3_cell}")
    
    # If the key doesn't exist or expired (TTL), demand is 0
    if demand is None:
        return {"h3_cell": h3_cell, "demand": 0}
        
    return {"h3_cell": h3_cell, "demand": int(demand)}

@app.get("/demand")
def get_all_active_demand():
    """
    Fetch all active hexagons on the map.
    """
    keys = r.keys("demand:*")
    if not keys:
        return {"data": []}
    
    values = r.mget(keys)
    
    results = []
    for key, val in zip(keys, values):
        h3_id = key.split(":")[1]
        results.append({
            "h3_cell": h3_id, 
            "demand": int(val)
        })
        
    return {"data": results}

# http://0.0.0.0:8000/demand
# http://0.0.0.0:8000/docs