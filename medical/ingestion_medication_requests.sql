"""
MedicationRequest Resource Sync Script for Google Cloud
Writes data directly to BigQuery
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse, parse_qs, urlencode
import requests
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import secretmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ResourceSync:
    def __init__(self, config: Dict[str, str]):
        """
        Initialize the sync client
        
        Args:
            config: Dictionary containing configuration parameters
        """
        self.base_url = config.get('base_url', '<censored>')
        self.client_id = config['<censored>']
        self.client_secret = config['<censored>']
        
        # BigQuery configuration
        self.project_id = config.get('<censored>', '<censored>')
        self.dataset_id = config.get('<censored>', '<censored>')
        self.table_id = config.get('<censored>', 'medication_requests')
        self.dataset_location = config.get('<censored>', '<censored>')
        
        # State storage configuration
        self.bucket_name = config.get('state_bucket', '<censored>')
        self.state_file_name = f"{self.table_id}_sync_state.json"
        
        self.access_token = None
        self.bigquery_client = bigquery.Client(project=self.project_id)
        self.storage_client = storage.Client()
        self.bucket = self.storage_client.bucket(self.bucket_name)
        
    def get_access_token(self) -> str:
        """
        Obtain OAuth2 access token using client credentials
        """
        token_url = f"{self.base_url}/oauth2/token"
        
        data = {
            'grant_type': 'client_credentials',
            'client_id': self.client_id,
            'client_secret': self.client_secret
        }
        
        response = requests.post(token_url, data=data)
        response.raise_for_status()
        
        token_data = response.json()
        self.access_token = token_data['access_token']
        logger.info("Successfully obtained access token")
        
        return self.access_token
    
    def load_sync_state(self) -> Dict[str, Any]:
        """
        Load the last sync state from Cloud Storage
        """
        try:
            blob = self.bucket.blob(self.state_file_name)
            if blob.exists():
                state_data = blob.download_as_text()
                state = json.loads(state_data)
                logger.info(f"Loaded sync state: {state}")
                return state
        except Exception as e:
            logger.warning(f"Could not load sync state: {e}")
        
        # Default state with initial date
        return {
            'last_sync_time': '2021-01-01T00:00:00.000Z',
            'records_synced': 0
        }
    
    def save_sync_state(self, state: Dict[str, Any]):
        """
        Save the current sync state to Cloud Storage
        """
        try:
            blob = self.bucket.blob(self.state_file_name)
            blob.upload_from_string(json.dumps(state, indent=2))
            logger.info(f"Saved sync state: {state}")
        except Exception as e:
            logger.error(f"Failed to save sync state: {e}")
            raise
    
    def ensure_bigquery_dataset(self):
        """
        Ensure BigQuery dataset exists
        """
        dataset_id = f"{self.project_id}.{self.dataset_id}"
        
        try:
            self.bigquery_client.get_dataset(dataset_id)
            logger.info(f"Dataset {dataset_id} already exists")
        except:
            # Create dataset if it doesn't exist
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = self.dataset_location
            
            dataset = self.bigquery_client.create_dataset(dataset, exists_ok=True)
            logger.info(f"Created dataset {dataset_id}")
    
    def ensure_bigquery_table(self):
        """
        Ensure BigQuery table exists with the correct schema
        """
        # First ensure dataset exists
        self.ensure_bigquery_dataset()
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Define schema based on the JSON schema provided
        schema = [
            bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("resourcetype", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("status", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("intent", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("authoredon", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("meta_versionid", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("meta_lastupdated", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("search_mode", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("fullurl", "STRING", mode="NULLABLE"),
            # Reference fields
            bigquery.SchemaField("subject_reference", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("subject_display", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("requester_reference", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("requester_display", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("performer_reference", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("performer_display", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recorder_reference", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("recorder_display", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("basedon_reference", "STRING", mode="NULLABLE"),
            # Identifier fields
            bigquery.SchemaField("identifier_system_1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("identifier_value_1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("identifier_system_2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("identifier_value_2", "STRING", mode="NULLABLE"),
            # Medication fields
            bigquery.SchemaField("medicationcodeableconcept_coding_system", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("medicationcodeableconcept_coding_code", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("medicationcodeableconcept_coding_display", "STRING", mode="NULLABLE"),
            # Dosage fields
            bigquery.SchemaField("dosageinstruction_patientinstruction", "STRING", mode="NULLABLE"),
            # Note fields
            bigquery.SchemaField("note_id_1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note_authorreference_reference_1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note_authorreference_display_1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note_text_1", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note_id_2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note_authorreference_reference_2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note_authorreference_display_2", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("note_text_2", "STRING", mode="NULLABLE"),
            # Dispense request fields
            bigquery.SchemaField("dispenserequest_numberofrepeatsallowed", "INTEGER", mode="NULLABLE"),
            bigquery.SchemaField("dispenserequest_quantity_value", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("dispenserequest_quantity_unit", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("dispenserequest_expectedsupplyduration_value", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("dispenserequest_expectedsupplyduration_unit", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("dispenserequest_performer_reference", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("dispenserequest_performer_display", "STRING", mode="NULLABLE"),
            # Extension fields
            bigquery.SchemaField("extension_id", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extension_url", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("extension_valuestring", "STRING", mode="NULLABLE"),
            # Sync metadata
            bigquery.SchemaField("_sync_timestamp", "TIMESTAMP", mode="REQUIRED"),
        ]
        
        table = bigquery.Table(table_ref, schema=schema)
        
        # Create table if it doesn't exist
        try:
            self.bigquery_client.get_table(table_ref)
            logger.info(f"Table {table_ref} already exists")
        except:
            table = self.bigquery_client.create_table(table)
            logger.info(f"Created table {table_ref}")
    
    def transform_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply transformations to a single record based on the MedicationRequest configuration
        """
        transformed = {}
        
        # Helper function to safely access nested values
        def safe_get(obj, path, default=None):
            try:
                for key in path:
                    obj = obj[key]
                return obj
            except (KeyError, IndexError, TypeError):
                return default
        
        # Apply AddFields transformations
        resource = record.get('resource', {})
        
        # Basic fields
        transformed['id'] = resource.get('id')
        transformed['resourcetype'] = resource.get('resourceType')
        transformed['status'] = resource.get('status')
        transformed['intent'] = resource.get('intent')
        transformed['authoredon'] = resource.get('authoredOn')
        
        # Meta fields
        meta = resource.get('meta', {})
        transformed['meta_versionid'] = meta.get('versionId')
        transformed['meta_lastupdated'] = meta.get('lastUpdated')
        
        # Search fields
        search = record.get('search', {})
        transformed['search_mode'] = search.get('mode')
        transformed['fullurl'] = record.get('fullUrl')
        
        # Reference fields
        subject = resource.get('subject', {})
        if subject:
            transformed['subject_reference'] = subject.get('reference')
            transformed['subject_display'] = subject.get('display')
        
        requester = resource.get('requester', {})
        if requester:
            transformed['requester_reference'] = requester.get('reference')
            transformed['requester_display'] = requester.get('display')
        
        performer = resource.get('performer', {})
        if performer:
            transformed['performer_reference'] = performer.get('reference')
            transformed['performer_display'] = performer.get('display')
        
        recorder = resource.get('recorder', {})
        if recorder:
            transformed['recorder_reference'] = recorder.get('reference')
            transformed['recorder_display'] = recorder.get('display')
        
        # BasedOn field
        based_on = resource.get('basedOn', [])
        if based_on and len(based_on) > 0:
            transformed['basedon_reference'] = based_on[0].get('reference')

        # Note fields (2 max)
        notes = resource.get('note', [])
        for i in range(min(2, len(notes))):
            note = notes[i]
            transformed[f'note_id_{i+1}'] = note.get('id')
            
            # Handle author fields
            author = note.get('authorReference')
            if author:
                transformed[f'note_authorreference_reference_{i+1}'] = author.get('reference')
                transformed[f'note_authorreference_display_{i+1}'] = author.get('display')
            else:
                transformed[f'note_authorreference_reference_{i+1}'] = None
                transformed[f'note_authorreference_display_{i+1}'] = None
    
            # Text is directly a string, not an object with 'div'
            transformed[f'note_text_{i+1}'] = note.get('text')
        
        # Identifier fields (only 2 for MedicationRequest)
        identifiers = resource.get('identifier', [])
        for i in range(min(2, len(identifiers))):
            identifier = identifiers[i]
            transformed[f'identifier_system_{i+1}'] = identifier.get('system')
            transformed[f'identifier_value_{i+1}'] = identifier.get('value')
        
        # Medication CodeableConcept fields
        med_concept = resource.get('medicationCodeableConcept', {})
        if med_concept:
            coding = med_concept.get('coding', [])
            if coding and len(coding) > 0:
                transformed['medicationcodeableconcept_coding_system'] = coding[0].get('system')
                code = coding[0].get('code')
                # Note: Schema shows this as potentially numeric, but keeping as string for safety
                transformed['medicationcodeableconcept_coding_code'] = str(code) if code else None
                transformed['medicationcodeableconcept_coding_display'] = coding[0].get('display')
        
        # Dosage instruction fields
        dosage_instructions = resource.get('dosageInstruction', [])
        if dosage_instructions and len(dosage_instructions) > 0:
            transformed['dosageinstruction_patientinstruction'] = dosage_instructions[0].get('patientInstruction')
        
        # Dispense request fields
        dispense_request = resource.get('dispenseRequest', {})
        if dispense_request:
            transformed['dispenserequest_numberofrepeatsallowed'] = dispense_request.get('numberOfRepeatsAllowed')
            
            quantity = dispense_request.get('quantity', {})
            if quantity:
                transformed['dispenserequest_quantity_value'] = quantity.get('value')
                transformed['dispenserequest_quantity_unit'] = quantity.get('unit')
            
            supply_duration = dispense_request.get('expectedSupplyDuration', {})
            if supply_duration:
                transformed['dispenserequest_expectedsupplyduration_value'] = supply_duration.get('value')
                transformed['dispenserequest_expectedsupplyduration_unit'] = supply_duration.get('unit')
            
            dispense_performer = dispense_request.get('performer', {})
            if dispense_performer:
                transformed['dispenserequest_performer_reference'] = dispense_performer.get('reference')
                transformed['dispenserequest_performer_display'] = dispense_performer.get('display')
        
        # Extension fields (only 1 for MedicationRequest)
        extensions = resource.get('extension', [])
        if extensions and len(extensions) > 0:
            extension = extensions[0]
            transformed['extension_id'] = extension.get('id')
            transformed['extension_url'] = extension.get('url')
            transformed['extension_valuestring'] = extension.get('valueString')
        
        # Add sync timestamp
        transformed['_sync_timestamp'] = datetime.now(timezone.utc).isoformat()
        
        # Clean up None values to match schema
        return {k: v for k, v in transformed.items() if v is not None}
    
    def fetch_medication_requests(self, last_updated: str) -> List[Dict[str, Any]]:
        """
        Fetch medication request records from <censored> API with pagination
        
        Args:
            last_updated: ISO datetime string for incremental sync
        """
        if not self.access_token:
            self.get_access_token()
        
        headers = {
            'Authorization': f'Bearer {self.access_token}',
            'Content-Type': 'application/json'
        }
        
        # Initial URL with query parameters
        params = {
            '_sort': '_lastUpdated',
            '_count': '500',
            '_lastUpdated': f'gt{last_updated}'
        }
        
        url = f"{self.base_url}/fhir/R4/MedicationRequest"
        all_records = []
        
        while url:
            # Make request
            if '?' in url:
                # URL already has parameters (from pagination)
                response = requests.get(url, headers=headers)
            else:
                # First request with parameters
                response = requests.get(url, headers=headers, params=params)
            
            if response.status_code == 401:
                # Token expired, refresh and retry
                logger.info("Token expired, refreshing...")
                self.get_access_token()
                headers['Authorization'] = f'Bearer {self.access_token}'
                continue
            
            response.raise_for_status()
            data = response.json()
            
            # Extract entries
            entries = data.get('entry', [])
            all_records.extend(entries)
            logger.info(f"Fetched {len(entries)} records, total: {len(all_records)}")
            
            # Find next URL from links
            url = None
            links = data.get('link', [])
            for link in links:
                if link.get('relation') == 'next':
                    url = link.get('url')
                    break
            
            if url and not url.startswith('http'):
                # Handle relative URLs
                url = f"{self.base_url}{url}"
        
        return all_records
    
    def upsert_to_bigquery(self, records: List[Dict[str, Any]]):
        """
        Upsert records to BigQuery table using MERGE with a temporary table
        """
        if not records:
            return
        
        table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
        
        # Create a unique temporary table name with timestamp
        temp_table_id = f"{self.table_id}_temp_{int(datetime.now().timestamp())}"
        temp_table_ref = f"{self.project_id}.{self.dataset_id}.{temp_table_id}"
        
        try:
            # Get the schema of the target table
            try:
                target_table = self.bigquery_client.get_table(table_ref)
                schema = target_table.schema
            except Exception as e:
                logger.warning(f"Target table does not exist, creating it: {e}")
                self.ensure_bigquery_table()
                target_table = self.bigquery_client.get_table(table_ref)
                schema = target_table.schema
            
            # Create temporary table with the same schema
            temp_table = bigquery.Table(temp_table_ref, schema=schema)
            temp_table = self.bigquery_client.create_table(temp_table)
            logger.info(f"Created temporary table {temp_table_ref}")
            
            # Ensure all records have the _sync_timestamp field set to current time
            current_timestamp = datetime.now(timezone.utc).isoformat()
            for record in records:
                # Always update the sync timestamp to the current time
                record['_sync_timestamp'] = current_timestamp
            
            # Load records into the temporary table using a load job instead of streaming
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
                schema=schema,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                ignore_unknown_values=True,  # Ignore extra fields not in schema
                max_bad_records=0  # Fail if any records are bad
            )
            
            # Convert records to NDJSON format for load job
            import tempfile
            import json
            
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
                for record in records:
                    json.dump(record, tmp_file)
                    tmp_file.write('\n')
                tmp_file_path = tmp_file.name
            
            try:
                # Load the data from the temporary file
                with open(tmp_file_path, 'rb') as source_file:
                    load_job = self.bigquery_client.load_table_from_file(
                        source_file,
                        temp_table_ref,
                        job_config=job_config
                    )
                
                # Wait for the load job to complete
                load_job.result()
                logger.info(f"Loaded {len(records)} records into temporary table via load job")
                
            finally:
                # Clean up the temporary file
                import os
                os.unlink(tmp_file_path)
            
            # Build the MERGE statement with properly escaped column names
            # Get column names from schema, excluding 'id' but including '_sync_timestamp'
            regular_columns = [field.name for field in schema 
                            if field.name != 'id' and not field.name.startswith('_')]
            
            # Make sure _sync_timestamp is included
            all_columns = regular_columns + ['_sync_timestamp']
            
            # Build the UPDATE SET clause with backticks for all column names
            update_clause = ", ".join([f"target.`{col}` = source.`{col}`" for col in all_columns])
            
            # Build the INSERT columns and VALUES clauses with backticks
            insert_columns = ", ".join([f"`{col}`" for col in ['id'] + all_columns])
            insert_values = ", ".join([f"source.`{col}`" for col in ['id'] + all_columns])
            
            # Execute the MERGE statement with escaped column names
            merge_query = f"""
                MERGE `{table_ref}` AS target
                USING `{temp_table_ref}` AS source
                ON target.`id` = source.`id`
                WHEN MATCHED THEN
                    UPDATE SET {update_clause}
                WHEN NOT MATCHED THEN
                    INSERT ({insert_columns})
                    VALUES ({insert_values})
            """
            
            merge_job = self.bigquery_client.query(merge_query)
            merge_result = merge_job.result()
            
            logger.info(f"Successfully merged {len(records)} records into {table_ref}")
            
        except Exception as e:
            logger.error(f"Failed during upsert operation: {e}")
            raise
        finally:
            # Always attempt to clean up the temporary table
            try:
                self.bigquery_client.delete_table(temp_table_ref)
                logger.info(f"Deleted temporary table {temp_table_ref}")
            except Exception as e:
                logger.warning(f"Failed to delete temporary table: {e}")
    
    def sync_medication_requests(self):
        """
        Main sync function that orchestrates the entire process
        """
        try:
            # Load sync state for metadata tracking purposes only
            state = self.load_sync_state()
            
            # Default start date if table doesn't exist
            default_start_time = '2021-01-01T00:00:00.000Z'
            last_sync_time = default_start_time
            
            # Check if table exists and get max meta_lastupdated
            table_ref = f"{self.project_id}.{self.dataset_id}.{self.table_id}"
            try:
                self.bigquery_client.get_table(table_ref)
                logger.info(f"Table {table_ref} exists, querying for max meta_lastupdated")
                
                # Query for the max meta_lastupdated value
                query = f"""
                    SELECT MAX(meta_lastupdated) as max_updated
                    FROM `{table_ref}`
                    WHERE meta_lastupdated IS NOT NULL
                """
                query_job = self.bigquery_client.query(query)
                results = query_job.result()
                
                for row in results:
                    if row.max_updated:
                        last_sync_time = row.max_updated
                        logger.info(f"Using max meta_lastupdated from table: {last_sync_time}")
                    else:
                        logger.info(f"No meta_lastupdated values found in table, using default: {default_start_time}")
            except Exception as e:
                logger.info(f"Table {table_ref} does not exist or error occurred: {e}")
                # Table doesn't exist, ensure it's created
                self.ensure_bigquery_table()
                logger.info(f"Using default start date for full load: {default_start_time}")
            
            logger.info(f"Starting sync from {last_sync_time}")
            
            # Fetch new/updated medication requests
            raw_records = self.fetch_medication_requests(last_sync_time)
            
            if not raw_records:
                logger.info("No new records to sync")
                return
            
            # Transform records
            transformed_records = []
            latest_update_time = last_sync_time
            
            for record in raw_records:
                transformed = self.transform_record(record)
                transformed_records.append(transformed)
                
                # Track latest update time
                record_time = transformed.get('meta_lastupdated', '')
                if record_time > latest_update_time:
                    latest_update_time = record_time
            
            # Upsert to BigQuery
            self.upsert_to_bigquery(transformed_records)
            
            # Update sync state (for metadata tracking only)
            new_state = {
                'last_sync_time': latest_update_time,  # This is just for reference now
                'records_synced': state.get('records_synced', 0) + len(transformed_records),
                'last_sync_run': datetime.now(timezone.utc).isoformat(),
                'last_records_count': len(transformed_records)
            }
            self.save_sync_state(new_state)
            
            logger.info(f"Sync completed successfully. Processed {len(transformed_records)} records")
            
        except Exception as e:
            logger.error(f"Sync failed: {e}")
            raise


def get_secret(secret_id: str, project_id: str) -> str:
    """
    Retrieve secret from Google Secret Manager
    """
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")


def main():
    """
    Main entry point for the sync script
    """
    # Get project ID for Secret Manager
    project_id = os.environ.get('<censored>', '<censored>')
    
    # Configuration
    config = {
        'base_url': os.environ.get('<censored>', '<censored>'),
        'project_id': os.environ.get('<censored>', '<censored>'),
        'dataset_id': os.environ.get('<censored>', '<censored>'),
        'table_id': os.environ.get('<censored>', '<censored>'),
        'dataset_location': os.environ.get('<censored>', '<censored>'),
        'state_bucket': os.environ.get('<censored>', '<censored>')
    }
    
    # Get credentials from Secret Manager
    try:
        config['client_id'] = get_secret('<censored>', project_id)
        config['client_secret'] = get_secret('<censored>', project_id)
        logger.info("Successfully retrieved credentials from Secret Manager")
    except Exception as e:
        logger.error(f"Failed to retrieve secrets from Secret Manager: {e}")
        # Fall back to environment variables if secrets not found
        config['client_id'] = os.environ.get('<censored>')
        config['client_secret'] = os.environ.get('<censored>')
        if config['client_id'] and config['client_secret']:
            logger.warning("Using credentials from environment variables")
    
    # Validate configuration
    if not config['client_id'] or not config['client_secret']:
        raise ValueError("Missing required configuration: client_id and client_secret not found in Secret Manager or environment variables")
    
    # Run sync
    sync_client = ResourceSync(config)
    sync_client.sync_medication_requests()


if __name__ == "__main__":
    main()
