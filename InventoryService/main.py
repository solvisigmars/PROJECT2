from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
import json
from pathlib import Path
import pika
import threading

app = FastAPI(title="InventoryService")

INVENTORY_FILE = Path("/app/inventory_data/inventory.json")
RABBITMQ_HOST = "rabbitmq"
PAYMENT_SUCCESS_QUEUE = "payment_success"
PAYMENT_FAILURE_QUEUE = "payment_failure"

class ProductRequest(BaseModel):
    merchant_id: str
    product_name: str
    price: float
    quantity: int

class ReserveRequest(BaseModel):
    amount: int

def init_inventory_file():
    INVENTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not INVENTORY_FILE.exists():
        with open(INVENTORY_FILE, "w") as f:
            json.dump({"products": []}, f)
init_inventory_file()

@app.post("/products", status_code=201)
def create_product(product: ProductRequest):
    with open(INVENTORY_FILE, "r+") as f:
        data = json.load(f)
        new_id = len(data["products"]) + 1
        new_product = {
            "id": new_id,
            "merchantId": product.merchant_id,
            "productName": product.product_name,
            "price": product.price,
            "quantity": product.quantity,
            "reserved": 0
        }
        data["products"].append(new_product)
        f.seek(0)
        json.dump(data, f, indent=4)
    return {"productId":new_id}

@app.get("/products/{product_id}", status_code=200)
def get_product(product_id: int):
    with open(INVENTORY_FILE, "r") as f:
        data = json.load(f)
        for product in data["products"]:
            if product["id"] == product_id:
                products_copy = product.copy()
                del products_copy["id"]
                return products_copy
        raise HTTPException(status_code=404, detail="Product does not exist")
    
@app.post("/products/{product_id}/reserve", status_code=200)
def reserve_product(product_id: int, request: ReserveRequest):
    amount = request.amount
    with open(INVENTORY_FILE, "r+") as f:
        data = json.load(f)
        for product in data["products"]:
            if product["id"] == product_id:
                available = product["quantity"] - product["reserved"]
                if available < amount:
                    raise HTTPException(status_code=400, detail="Not enough stock to reserve")
                
                product["reserved"] += amount
                f.seek(0)
                json.dump(data, f, indent=4)
                return {"message": f"Reserved {amount} item(s) of product {product_id}"}
        
        raise HTTPException(status_code=404, detail="Product does not exist")

def update_inventory_on_success(event):
    product_id = event.get("productId")
    with open(INVENTORY_FILE, "r+") as f:
        data = json.load(f)
        for product in data["products"]:
            if product["id"] == product_id:
                product["quantity"] -= product["reserved"]
                product["reserved"] = 0
                f.seek(0)
                json.dump(data, f, indent=4)
                return

def update_inventory_on_failure(event):
    product_id = event.get("productId")
    with open(INVENTORY_FILE, "r+") as f:
        data = json.load(f)
        for product in data["products"]:
            if product["id"] == product_id:
                product["reserved"] = 0
                f.seek(0)
                json.dump(data, f, indent=4)
                return

def consume_events():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=PAYMENT_SUCCESS_QUEUE)
    channel.queue_declare(queue=PAYMENT_FAILURE_QUEUE)

    def callback(ch, method, properties, body):
        event = json.loads(body)
        if method.routing_key == PAYMENT_SUCCESS_QUEUE:
            update_inventory_on_success(event)
        elif method.routing_key == PAYMENT_FAILURE_QUEUE:
            update_inventory_on_failure(event)

    channel.basic_consume(queue=PAYMENT_SUCCESS_QUEUE, on_message_callback=callback, auto_ack=True)
    channel.basic_consume(queue=PAYMENT_FAILURE_QUEUE, on_message_callback=callback, auto_ack=True)
    print("InventoryService listening for payment events...")
    channel.start_consuming()

@app.on_event("startup")
def startup_event():
    consumer_thread = threading.Thread(target=consume_events, daemon=True)
    consumer_thread.start()
    print("InventoryService is running")