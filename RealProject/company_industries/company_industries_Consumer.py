import json
import boto3
from kafka import KafkaConsumer
import time

# AWS S3 setup
s3 = boto3.client('s3')

# Parameters for S3
destination_bucket_name = 'udaykafkaproject1'  # Your destination S3 bucket name
destination_file_key = 'Consumer_data/company_industries_Consumer/'  # S3 key for the output file

# Kafka Consumer setup
consumer = KafkaConsumer(
    'company_industries_Producer',  # Kafka topic to subscribe to
    bootstrap_servers='pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',  # Adjust as needed
    sasl_plain_username='3CLEYYSSJBIOAWFK',  # Replace with actual username
    sasl_plain_password='w1K0fjfOBrm5nC4F1vRCn7WEoDX3T0/OtJHYT9BJcrMmx/fg4+v6LBGAW0jFmdTC',  # Replace with actual password
    auto_offset_reset='earliest',  # Start reading from the earliest message
    enable_auto_commit=True,  # Automatically commit offsets
    group_id='company_industries_Consumer'  # Your consumer group ID
)

# Prepare to write messages to a list
messages_to_write = []
batch_size = 100  # Define your preferred batch size

try:
    print("Listening for messages...")
    for message in consumer:
        # Deserialize the message value from bytes to string and then to JSON
        message_value = json.loads(message.value.decode('utf-8'))
        messages_to_write.append(message_value)
        print(f"Received message: {message_value}")

        # Write to S3 when batch size is reached
        if len(messages_to_write) >= batch_size:
            # Construct a unique S3 key for each batch
            timestamp = int(time.time())
            batch_file_key = f"{destination_file_key}{timestamp}_batch.json"
            
            # Write collected messages to S3 as JSON
            s3.put_object(
                Bucket=destination_bucket_name,
                Key=batch_file_key,
                Body=json.dumps(messages_to_write).encode('utf-8')
            )
            print(f"Successfully written {len(messages_to_write)} messages to S3 bucket '{destination_bucket_name}' under '{batch_file_key}'.")
            
            # Clear the list after writing to S3
            messages_to_write.clear()

except Exception as e:
    print(f"Error: {e}")

finally:
    # Close the consumer when done
    consumer.close()
