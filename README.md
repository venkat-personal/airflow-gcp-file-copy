# airflow-gcp-file-copy

To accomplish your requirements—reading a file from a Google Cloud Storage (GCS) bucket, decrypting it using a PGP private key, and then re-encrypting it using a PGP public key before storing it in a destination GCS bucket—you can use the following Airflow DAG. This DAG will read files, perform the decryption and encryption steps, and upload the transformed files to the destination bucket.

Required Libraries
You'll need the following Python libraries:

pgpy: For PGP decryption and encryption.
google-cloud-storage: To interact with Google Cloud Storage.
Ensure these libraries are installed in your environment:

```bash
pip install pgpy google-cloud-storage
```

Explanation
### PGP Decryption:
  #### PGPKey.from_blob(): 
  * Loads the PGP private key from a file.
  #### private_key.unlock(): 
  * Unlocks the private key using the provided passphrase.
  #### PGPMessage.from_blob(): 
  * Reads the encrypted content and creates a PGPMessage.
  #### private_key.decrypt(): 
  * Decrypts the message using the private key.
  
### PGP Encryption:
  #### PGPKey.from_blob(): 
  * Loads the PGP public key from a file.
  #### PGPMessage.new(): 
  * Wraps the decrypted content into a PGPMessage object.
  #### public_key.encrypt(): 
  * Encrypts the message using the public key.
### File Upload:
  The PGP-encrypted content is uploaded to the destination GCS bucket using upload_from_string().
