# Import the Google Cloud client library and JSON library
from google.cloud import storage
import json


class ReadJson():
    
    # Instantiating Google Cloud Storage client    
    def __init__(self):
        self.gcs_client = storage.Client()
        
    
    # Method to read & return metadata from the json file in the specified bucket path
    def get_metadata(self):
        # Configuring bucket & metadata json blob with storage client        
        bucket = self.gcs_client.get_bucket('bucket_name')
        blob = bucket.blob('GCSbucketpath/gold_metadata.json')
        
        # Download the contents of the blob as a string and then parse it using json.loads() method
        metadata = json.loads(blob.download_as_string(client=None))

        # Metadata variables        
        silver_meta = metadata['silver']
        gold_meta = metadata['gold']
        
        return silver_meta, gold_meta