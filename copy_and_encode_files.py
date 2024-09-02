import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from google.cloud import storage
from pgpy import PGPKey, PGPMessage
from datetime import timedelta

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Function to decrypt and then PGP encrypt files
def pgp_decrypt_and_encrypt_files(source_bucket_name, destination_bucket_name, pgp_private_key_path, pgp_passphrase, pgp_public_key_path, prefix=''):
    client = storage.Client()
    
    source_bucket = client.bucket(source_bucket_name)
    destination_bucket = client.bucket(destination_bucket_name)
    
    # Load the PGP private key
    with open(pgp_private_key_path, 'r') as key_file:
        private_key = PGPKey.from_blob(key_file.read())[0]
    
    # Unlock the private key with the passphrase
    with private_key.unlock(pgp_passphrase):
        # Load the PGP public key
        with open(pgp_public_key_path, 'r') as pub_key_file:
            public_key = PGPKey.from_blob(pub_key_file.read())[0]
        
        # List blobs in the source bucket with the given prefix
        blobs = source_bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            # Step 1: Read the content of the source file
            encrypted_content = blob.download_as_string()
            
            # Step 2: Decrypt the content using the PGP private key
            message = PGPMessage.from_blob(encrypted_content)
            decrypted_content = private_key.decrypt(message).message
            
            # Step 3: Encrypt the decrypted content using the PGP public key
            new_message = PGPMessage.new(decrypted_content)
            encrypted_message = public_key.encrypt(new_message)
            
            # Step 4: Store the PGP encrypted content into the destination bucket
            destination_blob = destination_bucket.blob(blob.name)
            destination_blob.upload_from_string(str(encrypted_message))

# Define the DAG
with DAG(
    dag_id='pgp_decrypt_and_encrypt_files_between_gcs_buckets',
    default_args=default_args,
    description='A DAG to PGP decrypt files, re-encrypt with a different PGP key, and store them in another GCS bucket',
    schedule_interval=None,  # Set the schedule interval to None to trigger manually
    start_date=days_ago(1),
    catchup=False,
    tags=['gcs', 'pgp', 'decrypt', 'encrypt'],
) as dag:

    # Define the task to decrypt and re-encrypt files
    pgp_decrypt_and_encrypt_task = PythonOperator(
        task_id='pgp_decrypt_and_encrypt_task',
        python_callable=pgp_decrypt_and_encrypt_files,
        op_kwargs={
            'source_bucket_name': os.getenv('SOURCE_BUCKET_NAME'),  # Retrieve from Airflow environment variables
            'destination_bucket_name': os.getenv('DESTINATION_BUCKET_NAME'),  # Retrieve from Airflow environment variables
            'pgp_private_key_path': os.getenv('PGP_PRIVATE_KEY_PATH'),  # Retrieve from Airflow environment variables
            'pgp_passphrase': os.getenv('PGP_PASSPHRASE'),  # Retrieve from Airflow environment variables
            'pgp_public_key_path': os.getenv('PGP_PUBLIC_KEY_PATH'),  # Retrieve from Airflow environment variables
            'prefix': os.getenv('PREFIX', ''),  # Optional prefix, default to an empty string
        },
    )

    pgp_decrypt_and_encrypt_task
