import json
from pathlib import Path
import pika
import time

RABBITMQ_HOST = "rabbitmq"
EXCHANGE = "events"

PAYMENTS_FILE = Path("/app/payments.json")

# Create payments file if not exists
if not PAYMENTS_FILE.exists():
    with open(PAYMENTS_FILE, "w") as f:
        json.dump({"payments": []}, f, indent=4)

def save_payment(order_id, status):
    with open(PAYMENTS_FILE, "r+") as f:
        data = json.load(f)
        data["payments"].append({"orderId": order_id, "status": status})
        f.seek(0)
        json.dump(data, f, indent=4)

def luhn_check(card_number: str) -> bool:
    digits = [int(d) for d in card_number if d.isdigit()]
    checksum = 0
    double = False
    for d in reversed(digits):
        if double:
            d = d * 2
            if d > 9:
                d -= 9
        checksum += d
        double = not double
    return checksum % 10 == 0

def publish_event(event_type, data):
    conn = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    ch = conn.channel()
    ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)
    ch.basic_publish(exchange=EXCHANGE, routing_key=event_type, body=json.dumps(data))
    conn.close()

def handle_order_created(order_event):
    order_id = order_event["orderId"]
    card = order_event["card"]

    ok = (
        luhn_check(card["cardNumber"])
        and 1 <= int(card["expirationMonth"]) <= 12
        and len(str(card["expirationYear"])) == 4
        and len(str(card["cvc"])) == 3
    )

    status = "success" if ok else "failed"
    save_payment(order_id, status)

    publish_event(f"payment.{status}", {
        "type": f"payment.{status}",
        "orderId": order_id,
        "productId": order_event["productId"],
        "buyerEmail": order_event["buyerEmail"],
        "merchantEmail": order_event["merchantEmail"]
    })

def consume_order_created():
    while True:
        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
            ch = conn.channel()
            ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

            queue = "payment.q"
            ch.queue_declare(queue=queue, durable=True)
            ch.queue_bind(queue=queue, exchange=EXCHANGE, routing_key="order_created")

            print("PaymentService listening for order.created events...")

            def callback(ch, method, properties, body):
                event = json.loads(body)
                handle_order_created(event)
                ch.basic_ack(method.delivery_tag)

            ch.basic_qos(prefetch_count=1)
            ch.basic_consume(queue=queue, on_message_callback=callback)
            ch.start_consuming()

        except Exception as e:
            print("PaymentService waiting for RabbitMQ...", e)
            time.sleep(3)

if __name__ == "__main__":
    consume_order_created()
