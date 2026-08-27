import os
import json
import base64
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import bigquery, storage

app = Flask(__name__)
bq_client = bigquery.Client()
storage_client = storage.Client()

PROJECT_ID = bq_client.project
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
    if "data" not in pubsub_message:
        return jsonify({"error": "Empty payload"}), 400

    # 1. Parse GCS Event Metadata Pointer
    try:
        event_data = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))
        bucket_name = event_data["bucket"]
        file_name = event_data["name"]
    except Exception as e:
        return jsonify({"error": f"Failed to parse GCS metadata: {str(e)}"}), 400

    # 2. Stream File from GCS
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()
        
        # Handle line-delimited JSON or standard JSON arrays
        if content.strip().startswith("["):
            records = json.loads(content)
        else:
            records = [json.loads(line) for line in content.strip().split("\n") if line]
    except Exception as e:
        dlq_row = [{
            "dlq_id": f"dlq_{datetime.utcnow().timestamp()}",
            "raw_payload": f"GCS Path: gs://{bucket_name}/{file_name}",
            "source_file": file_name,
            "rejection_reason": f"File Read Failure: {str(e)}",
            "status": "UNRESOLVED",
            "error_timestamp": datetime.utcnow().isoformat()
        }]
        bq_client.insert_rows_json(SILVER_DLQ_TABLE, dlq_row)
        return jsonify({"status": "file_read_error_routed_to_dlq"}), 200

    # 3. Validate & Micro-Batch Insert into BigQuery
    clean_batch = []
    dlq_batch = []

    for payload in records:
        is_valid, reason = validate_order(payload)
        if is_valid:
            quality_flag = "HIGH_VALUE" if float(payload.get("total_amount", 0)) >= 500 else "STANDARD"
            clean_batch.append({
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
            })
        else:
            dlq_batch.append({
                "dlq_id": f"dlq_{payload.get('order_id', datetime.utcnow().timestamp())}",
                "raw_payload": json.dumps(payload),
                "source_file": file_name,
                "rejection_reason": reason,
                "status": "UNRESOLVED",
                "error_timestamp": datetime.utcnow().isoformat()
            })

    if clean_batch:
        bq_client.insert_rows_json(SILVER_CLEAN_TABLE, clean_batch)
    if dlq_batch:
        bq_client.insert_rows_json(SILVER_DLQ_TABLE, dlq_batch)

    return jsonify({"status": "success", "processed_records": len(records)}), 200


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