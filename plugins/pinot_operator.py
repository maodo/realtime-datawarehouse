import glob
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests


class PinotSubmitSchemaOperator(BaseOperator):

    @apply_defaults
    def __init__(self,folder_path,pinot_url, *args, **kwargs):
        super(PinotSubmitSchemaOperator, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context):
        try:
            schema_files = glob.glob(self.folder_path + '/*.json')
            for schema in schema_files:
                with open(schema, 'r') as f:
                    schema_data = f.read()
                    # Header and submit to pinot
                    headers = {'Content-Type':'application/json'}
                    response = requests.post(self.pinot_url,headers=headers,data=schema_data)
                    if response.status_code == 200:
                        self.log.info(f'Schema successfully submitted to Pinot : {schema}')
                    else:
                        self.log.error(f'An error occurred while submitting {response.status_code - response.text}')
                        raise Exception('Schema submition failed')
        except Exception as e:
            self.log.error(f'An error occurred while executing {str(e)}')