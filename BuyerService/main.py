from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
from pathlib import Path

app = FastAPI(title="BuyerService")

BUYER_FILE = Path("/app/buyer_data/buyers.json")

class BuyerRequest(BaseModel):
    name: str
    ssn: str
    email: str
    phoneNumber: str

def init_buyer_file():
    BUYER_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not BUYER_FILE.exists():
        with open(BUYER_FILE, "w") as f:
            json.dump({"buyers": []}, f)


init_buyer_file()

@app.post("/buyers", status_code=201)
def create_buyer(buyer: BuyerRequest):
    with open(BUYER_FILE, "r+") as f:
        data = json.load(f)
        new_id = len(data["buyers"]) + 1
        new_buyer = {
            "id": new_id,
            "name": buyer.name,
            "ssn": buyer.ssn,
            "email": buyer.email,
            "phoneNumber": buyer.phoneNumber
        }
        data["buyers"].append(new_buyer)
        f.seek(0)
        json.dump(data, f, indent=4)
    return {"buyerId": new_id}

@app.get("/buyers/{buyer_id}", status_code=200)
def get_buyer(buyer_id: int):
    with open(BUYER_FILE, "r") as f:
        data = json.load(f)
        for buyer in data ["buyers"]:
            if buyer["id"] == buyer_id:
                buyer_copy = buyer.copy()
                del buyer_copy["id"]
                return buyer_copy
        raise HTTPException(status_code=404, detail="Buyer does not exist")