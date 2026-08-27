import os
import json
import base64
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import bigquery

app = Flask(__name__)
bq_client = bigquery.Client()

PROJECT_ID = os.getenv("GCP_PROJECT", bq_client.project)
SILVER_CLEAN_TABLE = f"{PROJECT_ID}.silver_staging.clean_orders"
SILVER_DLQ_TABLE = f"{PROJECT_ID}.silver_staging.dlq_orders"


def validate_order(data):
    required_fields = ["order_id", "customer_id", "subtotal_amount", "total_amount"]
    for field in required_fields:
        if field not in data or data[field] is None:
            return False, f"Missing required field: {field}"

    subtotal = float(data.get("subtotal_amount", 0))
    tax = float(data.get("tax_amount", 0))
    shipping = float(data.get("shipping_fee", 0))
    discount = float(data.get("discount_amount", 0))
    total = float(data.get("total_amount", 0))

    expected_total = round(subtotal + tax + shipping - discount, 2)
    if round(total, 2) != expected_total:
        return False, f"Financial mismatch: expected total {expected_total}, got {total}"

    return True, "VALID"


@app.route("/process", methods=["POST"])
def process_event():
    envelope = request.get_json()
    if not envelope or "message" not in envelope:
        return jsonify({"error": "Invalid Pub/Sub payload"}), 400

    pubsub_message = envelope["message"]
    if "data" in pubsub_message:
        raw_decoded = base64.b64decode(pubsub_message["data"]).decode("utf-8")
        try:
            payload = json.loads(raw_decoded)
        except Exception as e:
            dlq_row = [{
                "dlq_id": f"dlq_{datetime.utcnow().timestamp()}",
                "raw_payload": raw_decoded,
                "source_file": "pubsub_stream",
                "rejection_reason": f"Corrupted JSON: {str(e)}",
                "status": "UNRESOLVED",
                "error_timestamp": datetime.utcnow().isoformat()
            }]
            bq_client.insert_rows_json(SILVER_DLQ_TABLE, dlq_row)
            return jsonify({"status": "routed_to_dlq"}), 200
    else:
        return jsonify({"error": "Empty payload"}), 400

    is_valid, reason = validate_order(payload)

    if is_valid:
        quality_flag = "HIGH_VALUE" if float(payload.get("total_amount", 0)) >= 500 else "STANDARD"
        clean_row = [{
            "order_id": payload.get("order_id"),
            "order_group_id": payload.get("order_group_id"),
            "customer_id": payload.get("customer_id"),
            "store_region": payload.get("store_region", "UNKNOWN"),
            "currency": payload.get("currency", "USD"),
            "subtotal_amount": payload.get("subtotal_amount"),
            "tax_amount": payload.get("tax_amount", 0),
            "discount_amount": payload.get("discount_amount", 0),
            "shipping_fee": payload.get("shipping_fee", 0),
            "total_amount": payload.get("total_amount"),
            "payment_method": payload.get("payment_method"),
            "payment_status": payload.get("payment_status", "COMPLETED"),
            "fulfillment_status": payload.get("fulfillment_status", "PENDING"),
            "item_count": payload.get("item_count", 1),
            "device_type": payload.get("device_type", "UNKNOWN"),
            "ip_country": payload.get("ip_country", "UNKNOWN"),
            "quality_flag": quality_flag,
            "processed_at": datetime.utcnow().isoformat()
        }]
        bq_client.insert_rows_json(SILVER_CLEAN_TABLE, clean_row)
        return jsonify({"status": "success", "destination": "clean_orders"}), 200
    else:
        dlq_row = [{
            "dlq_id": f"dlq_{payload.get('order_id', datetime.utcnow().timestamp())}",
            "raw_payload": json.dumps(payload),
            "source_file": "pubsub_stream",
            "rejection_reason": reason,
            "status": "UNRESOLVED",
            "error_timestamp": datetime.utcnow().isoformat()
        }]
        bq_client.insert_rows_json(SILVER_DLQ_TABLE, dlq_row)
        return jsonify({"status": "success", "destination": "dlq_orders"}), 200


@app.route("/retry", methods=["POST"])
def retry_dlq():
    query = f"""
        SELECT dlq_id, raw_payload 
        FROM `{SILVER_DLQ_TABLE}`
        WHERE status IN ('PATCHED', 'UNRESOLVED')
        LIMIT 100
    """
    rows = list(bq_client.query(query).result())

    reprocessed_count = 0
    for row in rows:
        try:
            payload = json.loads(row["raw_payload"]) if isinstance(row["raw_payload"], str) else row["raw_payload"]
            is_valid, _ = validate_order(payload)

            if is_valid:
                quality_flag = "HIGH_VALUE" if float(payload.get("total_amount", 0)) >= 500 else "STANDARD"
                clean_row = [{
                    "order_id": payload.get("order_id"),
                    "order_group_id": payload.get("order_group_id"),
                    "customer_id": payload.get("customer_id"),
                    "store_region": payload.get("store_region", "UNKNOWN"),
                    "currency": payload.get("currency", "USD"),
                    "subtotal_amount": payload.get("subtotal_amount"),
                    "tax_amount": payload.get("tax_amount", 0),
                    "discount_amount": payload.get("discount_amount", 0),
                    "shipping_fee": payload.get("shipping_fee", 0),
                    "total_amount": payload.get("total_amount"),
                    "payment_method": payload.get("payment_method"),
                    "payment_status": payload.get("payment_status", "COMPLETED"),
                    "fulfillment_status": payload.get("fulfillment_status", "PENDING"),
                    "item_count": payload.get("item_count", 1),
                    "device_type": payload.get("device_type", "UNKNOWN"),
                    "ip_country": payload.get("ip_country", "UNKNOWN"),
                    "quality_flag": quality_flag,
                    "processed_at": datetime.utcnow().isoformat()
                }]
                
                bq_client.insert_rows_json(SILVER_CLEAN_TABLE, clean_row)
                
                update_sql = f"""
                    UPDATE `{SILVER_DLQ_TABLE}`
                    SET status = 'RETRIED'
                    WHERE dlq_id = '{row["dlq_id"]}'
                """
                bq_client.query(update_sql).result()
                reprocessed_count += 1
        except Exception:
            continue

    return jsonify({"status": "retry_complete", "reprocessed_records": reprocessed_count}), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))