import json
import os
import pika
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
from dotenv import load_dotenv





load_dotenv()

SENDGRID_API_KEY = os.getenv("SENDGRID_API_KEY")
SENDGRID_SENDER_EMAIL = os.getenv("SENDGRID_SENDER_EMAIL")

RABBITMQ_HOST = "rabbitmq"
EXCHANGE = "events"


def send_email(to_email, subject, body):
    message = Mail(
        from_email=SENDGRID_SENDER_EMAIL,
        to_emails=to_email,
        subject=subject,
        html_content=body
    )

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        sg.send(message)
        print(f"Email sent to {to_email}")
    except Exception as e:
        print(f"Failed to send email to {to_email}: {e}")


def handle_event(event):
    event_type = event.get("type")
    buyer_email = event.get("buyerEmail")
    merchant_email = event.get("merchantEmail")

    if event_type == "order.created":
        subject = "Order has been created"
        body = f"Order {event['orderId']} was created for product {event['productId']} costing {event['totalPrice']}."

    elif event_type == "payment.success":
        subject = "Order has been purchased"
        body = f"Order {event['orderId']} has been successfully purchased."

    elif event_type == "payment.failure":
        subject = "Order purchase failed"
        body = f"Order {event['orderId']} purchase has failed."

    else:
        return  # Ignore unknown events

    send_email(buyer_email, subject, body)
    send_email(merchant_email, subject, body)


def callback(ch, method, properties, body):
    event = json.loads(body)
    print(f"Received: {event}")
    handle_event(event)
    ch.basic_ack(method.delivery_tag)


def consume_events():
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST))
    channel = connection.channel()

    channel.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=True)

    # One queue shared across multiple EmailService containers
    channel.queue_declare(queue="email.q", durable=True)

    # Bind to relevant events
    channel.queue_bind(queue="email.q", exchange=EXCHANGE, routing_key="order.created")
    channel.queue_bind(queue="email.q", exchange=EXCHANGE, routing_key="payment.success")
    channel.queue_bind(queue="email.q", exchange=EXCHANGE, routing_key="payment.failure")

    channel.basic_consume(queue="email.q", on_message_callback=callback)

    print(" EmailService listening for events...")
    channel.start_consuming()


if __name__ == "__main__":
    consume_events()
