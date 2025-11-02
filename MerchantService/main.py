from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
from pathlib import Path

app = FastAPI(title="MerchantService")

MERCHANT_FILE = Path("/app/merchants.json")

class MerchantRequest(BaseModel):
    name: str
    ssn: str
    email: str
    phone_number: str
    allows_discount: bool

def init_merchant_file():
    if not MERCHANT_FILE.exists():
        with open(MERCHANT_FILE, "w") as f:
            json.dump({"merchants": []}, f)

init_merchant_file()
    
@app.post("/merchants", status_code=201)
def create_merchant(merchant: MerchantRequest):
    with open(MERCHANT_FILE, "r+") as f:
        data =json.load(f)
        new_id = len(data["merchants"]) + 1
        new_merchant = {
            "id": new_id,
            "name": merchant.name,
            "ssn": merchant.ssn,
            "email": merchant.email,
            "phoneNumber": merchant.phone_number,
            "allowsDiscount": merchant.allows_discount
        }
        data["merchants"].append(new_merchant)
        f.seek(0)
        json.dump(data, f, indent=4)
    return {"merchantId": new_id}

@app.get("/merchants/{merchant_id}", status_code=200)
def get_merchant(merchant_id: int):
    with open(MERCHANT_FILE, "r") as f:
        data = json.load(f)
        for merchant in data["merchants"]:
            if merchant["id"] == merchant_id:
                merchant_copy = merchant.copy()
                del merchant_copy["id"]
                return merchant_copy
        raise HTTPException(status_code=404, detail="Merchant does not exist")