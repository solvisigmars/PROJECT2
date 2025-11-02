from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import requests
from pathlib import Path
import pika


app = FastAPI(title="OrderService")

ORDERS_FILE = Path("/app/orders.json")
RABBITMQ_HOST = "rabbitmq"
QUEUE_NAME = "order_created"


class PaymentDetails(BaseModel):
    cardNumber: str
    expirationMonth: int
    expirationYear: int
    cvc: int

class OrderRequest(BaseModel):
    productId: int
    merchantId: int
    buyerId: int
    creditCard: PaymentDetails
    discount: float = 0.0

def init_orders_file():
    if not ORDERS_FILE.exists():
        with open(ORDERS_FILE, "w") as f:
            json.dump({"orders": []}, f)

init_orders_file()

def publish_order_event(order):
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(order)
    )
    connection.close()


@app.post("/orders", status_code=201)
def create_order(order: OrderRequest):
    # Validate if merchant, buyer and product exist
    merchant = requests.get(f"http://merchantservice:8001/merchants/{order.merchantId}", timeout = 3)
    if merchant.status_code != 200:
        raise HTTPException(status_code=400, detail="Merchant does not exist")
    merchant_data = merchant.json()

    buyer = requests.get(f"http://buyerservice:8002/buyers/{order.buyerId}", timeout = 3)
    if buyer.status_code != 200:
        raise HTTPException(status_code=400, detail="Buyer does not exist")
    
    product = requests.get(f"http://inventoryservice:8003/products/{order.productId}", timeout=3)
    if product.status_code != 200:
        raise HTTPException(status_code=400, detail="Product does not exist")
    product_data = product.json()

    if product_data["quantity"] <= product_data["reserved"]:
        raise HTTPException(status_code=400, detail="Product is sold out")
    if product_data["merchantId"] != order.merchantId:
        raise HTTPException(status_code=400, detail="Product does not belong to merchant")
    if not merchant_data["allowsDiscount"] and order.discount != 0:
        raise HTTPException(status_code=400, detail="Merchant does not allow discount")
    
    price = product_data["price"] * (1 - order.discount)

    card_number = order.creditCard.cardNumber
    masked_card = "*" * (len(card_number)- 4) + card_number[-4:]

    with open(ORDERS_FILE, "r+") as f:
        data = json.load(f)
        new_id = len(data["orders"]) + 1
        new_order = {
            "id": new_id,
            "productId": order.productId,
            "merchantId": order.merchantId,
            "buyerId": order.buyerId,
            "cardNumber": masked_card,
            "totalPrice": price
        }
        data["orders"].append(new_order)
        f.seek(0)
        json.dump(data, f, indent=4)

    publish_order_event(new_order)

    return {"orderId": new_id}

@app.get("/orders/{order_id}", status_code=200)
def get_order(order_id: int):
    with open(ORDERS_FILE, "r") as f:
        data = json.load(f)
        for order in data["orders"]:
            if order["id"] == order_id:
                return order
        raise HTTPException(status_code=404, detail="Order does not exist")