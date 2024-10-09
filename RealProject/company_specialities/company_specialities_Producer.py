import pandas as pd
import boto3
from io import StringIO
from kafka import KafkaProducer

# AWS S3 setup
s3 = boto3.client('s3')

# Parameters for S3
bucket_name = 'udaykafkaproject1'  # S3 bucket name
file_key = 'Linkedin DataSet/companies/company_specialities.csv'  # Updated S3 key for company specialities CSV file

# Try fetching CSV file from S3
try:
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')  # Read and decode CSV
    company_specialities = pd.read_csv(StringIO(csv_content))  # Convert to DataFrame
    print(company_specialities.head())  # Print first few rows to verify
except Exception as e:
    print(f"Error fetching file from S3: {e}")  # Print detailed error message

# Kafka setup
try:
    producer = KafkaProducer(
        bootstrap_servers='pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        security_protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_plain_username='3CLEYYSSJBIOAWFK',  # Replace with actual username
        sasl_plain_password='w1K0fjfOBrm5nC4F1vRCn7WEoDX3T0/OtJHYT9BJcrMmx/fg4+v6LBGAW0jFmdTC'  # Replace with actual password
    )
except Exception as e:
    print(f"Error connecting to Kafka: {e}")  # Print Kafka connection error
    exit(1)

# Sending messages to Kafka topic
for index, row in company_specialities.iterrows():
    # Convert each row to a JSON string
    message = row.to_json()
    
    # Send the message to Kafka
    try:
        producer.send('company_specialities_Producer', value=message.encode('utf-8'))  # Updated topic name
        print(f"Sent message: {message}")
    except Exception as e:
        print(f"Error sending message to Kafka: {e}")

# Don't forget to close the producer after sending messages
producer.close()
