import base64
import json
from datetime import datetime
from google.cloud import bigquery, storage

# Initialize GCP Clients
bq_client = bigquery.Client()
storage_client = storage.Client()

PROJECT_ID = bq_client.project
SILVER_CLEAN_TABLE = f"{PROJECT_ID}.silver_staging.clean_orders"
SILVER_DLQ_TABLE = f"{PROJECT_ID}.silver_staging.dlq_orders"


def validate_order(data):
    """Business validation and financial reconciliation check."""
    required_fields = ["order_id", "customer_id", "subtotal_amount", "total_amount"]
    for field in required_fields:
        if field not in data or data[field] is None:
            return False, f"Missing required field: {field}"

    try:
        subtotal = float(data.get("subtotal_amount", 0))
        tax = float(data.get("tax_amount", 0))
        shipping = float(data.get("shipping_fee", 0))
        discount = float(data.get("discount_amount", 0))
        total = float(data.get("total_amount", 0))

        expected_total = round(subtotal + tax + shipping - discount, 2)
        if round(total, 2) != expected_total:
            return False, f"Financial mismatch: expected total {expected_total}, got {total}"
    except Exception as e:
        return False, f"Data type conversion error: {str(e)}"

    return True, "VALID"


def process_event(request):
    """
    HTTP entrypoint function called by functions-framework / Cloud Run.
    Extracts GCS pointer from Pub/Sub payload, validates rows, and writes to BigQuery.
    """
    # 1. Parse incoming request body
    request_json = request.get_json(silent=True) if hasattr(request, "get_json") else request
    if not request_json:
        return ("Bad Request: Missing JSON payload", 400)

    # 2. Extract Pub/Sub envelope wrapper
    if isinstance(request_json, dict) and "message" in request_json:
        pubsub_message = request_json["message"]
        if "data" in pubsub_message:
            raw_message = base64.b64decode(pubsub_message["data"]).decode("utf-8")
            event_payload = json.loads(raw_message)
        else:
            return ("Bad Request: Empty Pub/Sub data payload", 400)
    elif isinstance(request_json, dict) and "data" in request_json:
        raw_message = base64.b64decode(request_json["data"]).decode("utf-8")
        event_payload = json.loads(raw_message)
    else:
        event_payload = request_json

    # 3. Extract GCS Bucket and Object reference
    bucket_name = event_payload.get("bucket")
    file_name = event_payload.get("name")

    if not bucket_name or not file_name:
        return ("Skipped: Not a valid GCS event notification", 200)

    # 4. Stream file contents from GCS
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()

        if content.strip().startswith("["):
            records = json.loads(content)
        else:
            records = [json.loads(line) for line in content.strip().split("\n") if line]
    except Exception as e:
        # Route unreadable or corrupted files directly to DLQ
        dlq_row = [{
            "dlq_id": f"dlq_{datetime.utcnow().timestamp()}",
            "raw_payload": f"GCS Path: gs://{bucket_name}/{file_name}",
            "source_file": file_name,
            "rejection_reason": f"File Parse Error: {str(e)}",
            "status": "UNRESOLVED",
            "error_timestamp": datetime.utcnow().isoformat()
        }]
        bq_client.insert_rows_json(SILVER_DLQ_TABLE, dlq_row)
        return ("Corrupted file routed to DLQ", 200)

    clean_batch = []
    dlq_batch = []

    # 5. Process and classify rows
    for payload in records:
        is_valid, reason = validate_order(payload)

        if is_valid:
            quality_flag = "HIGH_VALUE" if float(payload.get("total_amount", 0)) >= 500 else "STANDARD"
            clean_batch.append({
                "order_id": str(payload.get("order_id")),
                "order_group_id": payload.get("order_group_id"),
                "customer_id": str(payload.get("customer_id")),
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

    # 6. Bulk insert into Silver tables
    if clean_batch:
        bq_client.insert_rows_json(SILVER_CLEAN_TABLE, clean_batch)
    if dlq_batch:
        bq_client.insert_rows_json(SILVER_DLQ_TABLE, dlq_batch)

    return (f"Successfully processed {len(records)} records from {file_name}", 200)