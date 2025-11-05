import boto3
import csv
import io

s3 = boto3.client('s3')
BUCKET = "my-etl-pipeline-demo"

def lambda_handler(event, context):
    # 'results' comes from the Map state in Step Functions
    results = event.get("results", [])
    total_amount = 0
    summary = []

    for r in results:
        # Access the actual Lambda output inside 'Payload'
        payload = r.get("Payload", {})
        key = payload.get("processed_file")
        if not key:
            continue
        print(f"Processing file {key}")

        # Read the cleaned CSV from S3
        obj = s3.get_object(Bucket=BUCKET, Key=key)
        data = obj['Body'].read().decode('utf-8').splitlines()
        reader = csv.DictReader(data)

        # Calculate subtotal for this region
        subtotal = sum(float(row["amount"]) for row in reader)
        print(subtotal)
        # Append to summary
        summary.append({"region": payload["region"], "total_sales": subtotal})

        # Add to grand total
        total_amount += subtotal
        print(f"Total so far: {total_amount}")

    # Write summary CSV to S3
    summary_key = "output/summary/aggregated_report.csv"
    csv_buffer = io.StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=["region", "total_sales"])
    writer.writeheader()
    writer.writerows(summary)
    writer.writerow({"region": "TOTAL", "total_sales": total_amount})

    s3.put_object(Bucket=BUCKET, Key=summary_key, Body=csv_buffer.getvalue())

    # Return final summary
    return {
        "summary_file": summary_key,
        "total_sales": total_amount,
        "details": summary
    }
