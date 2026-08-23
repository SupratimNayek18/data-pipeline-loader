import base64
import json
from datetime import datetime, timezone
import functions_framework
from google.cloud import bigquery
from google.cloud import storage

bq_client = bigquery.Client()
storage_client = storage.Client()

@functions_framework.cloud_event
def subscribe(cloud_event):
    # 1. Decode Pub/Sub notification payload from GCS event
    pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    file_metadata = json.loads(pubsub_message)

    print(f"Received file metadata: {file_metadata}")
    
    bucket_name = file_metadata['bucket']
    file_name = file_metadata['name']
    
    # Process JSON files only
    if not file_name.endswith('.json'):
        print(f"Skipping non-JSON file: {file_name}")
        return

    # 2. Download file contents directly from GCS bucket
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_content = blob.download_as_text()

    clean_records = []
    dlq_records = []
    now = datetime.now(timezone.utc).isoformat()

    # 3. Rules Engine: Iterate and validate each row
    for line in file_content.strip().split('\n'):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            order_id = record.get('order_id')
            customer_id = record.get('customer_id')
            amount = record.get('amount')
            status = record.get('status', 'UNKNOWN')

            # Rule 1: Mandate valid IDs
            if not order_id or not customer_id:
                dlq_records.append({
                    "raw_record": line,
                    "rejection_reason": "MISSING_MANDATORY_KEYS",
                    "failed_at": now
                })
                continue

            # Rule 2: Mandate positive numeric amounts
            try:
                numeric_amount = float(amount)
                if numeric_amount <= 0:
                    raise ValueError("Amount <= 0")
            except (ValueError, TypeError):
                dlq_records.append({
                    "raw_record": line,
                    "rejection_reason": "INVALID_OR_NON_POSITIVE_AMOUNT",
                    "failed_at": now
                })
                continue

            # Rule 3 & 4: Transformation & Dynamic Quality Tagging
            quality_flag = "HIGH_VALUE" if numeric_amount > 5000 else "STANDARD"
            clean_records.append({
                "order_id": str(order_id),
                "customer_id": str(customer_id),
                "amount": numeric_amount,
                "status": str(status).upper(),
                "quality_flag": quality_flag,
                "processed_at": now
            })

        except Exception as e:
            dlq_records.append({
                "raw_record": line,
                "rejection_reason": f"JSON_PARSE_ERROR: {str(e)}",
                "failed_at": now
            })

    # 4. Route valid records to clean_orders table
    if clean_records:
        errors = bq_client.insert_rows_json("phase3_mastery.clean_orders", clean_records)
        if errors:
            print(f"Errors inserting clean records: {errors}")

    # 5. Route rejected records to DLQ table
    if dlq_records:
        errors = bq_client.insert_rows_json("phase3_mastery.dlq_orders", dlq_records)
        if errors:
            print(f"Errors inserting DLQ records: {errors}")

    print(f"Finished processing {file_name}: {len(clean_records)} clean, {len(dlq_records)} routed to DLQ.")