import base64
import json
from datetime import datetime
from google.cloud import bigquery, storage

# Lazy initialization placeholders (prevents startup crashes)
_bq_client = None
_storage_client = None

def get_bq_client():
    global _bq_client
    if not _bq_client:
        _bq_client = bigquery.Client()
    return _bq_client

def get_storage_client():
    global _storage_client
    if not _storage_client:
        _storage_client = storage.Client()
    return _storage_client

def validate_order(data):
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
    Cloud Functions / Cloud Run entrypoint.
    """
    try:
        request_json = request.get_json(silent=True) if hasattr(request, "get_json") else request
    except Exception:
        request_json = request

    if not request_json:
        return ("Bad Request: Empty payload", 400)

    # Parse Pub/Sub envelope wrapper safely
    event_payload = request_json
    if isinstance(request_json, dict):
        if "message" in request_json and "data" in request_json["message"]:
            try:
                decoded = base64.b64decode(request_json["message"]["data"]).decode("utf-8")
                event_payload = json.loads(decoded)
            except Exception:
                pass
        elif "data" in request_json:
            try:
                decoded = base64.b64decode(request_json["data"]).decode("utf-8")
                event_payload = json.loads(decoded)
            except Exception:
                pass

    bucket_name = event_payload.get("bucket")
    file_name = event_payload.get("name")

    if not bucket_name or not file_name:
        return ("Skipped: Not a valid GCS event notification", 200)

    bq = get_bq_client()
    st = get_storage_client()
    project_id = bq.project
    clean_table = f"{project_id}.silver_staging.clean_orders"
    dlq_table = f"{project_id}.silver_staging.dlq_orders"

    try:
        bucket = st.bucket(bucket_name)
        blob = bucket.blob(file_name)
        content = blob.download_as_text()

        records = json.loads(content) if content.strip().startswith("[") else [json.loads(line) for line in content.strip().split("\n") if line]
    except Exception as e:
        dlq_row = [{
            "dlq_id": f"dlq_{datetime.utcnow().timestamp()}",
            "raw_payload": f"GCS Path: gs://{bucket_name}/{file_name}",
            "source_file": file_name,
            "rejection_reason": f"File Read/Parse Error: {str(e)}",
            "status": "UNRESOLVED",
            "error_timestamp": datetime.utcnow().isoformat()
        }]
        bq.insert_rows_json(dlq_table, dlq_row)
        return ("File read error routed to DLQ", 200)

    clean_batch = []
    dlq_batch = []

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

    if clean_batch:
        bq.insert_rows_json(clean_table, clean_batch)
    if dlq_batch:
        bq.insert_rows_json(dlq_table, dlq_batch)

    return (f"Successfully processed {len(records)} records from {file_name}", 200)