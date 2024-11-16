import glob
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import requests


class PinotSubmitTableOperator(BaseOperator):

    @apply_defaults
    def __init__(self,folder_path,pinot_url, *args, **kwargs):
        super(PinotSubmitTableOperator, self).__init__(*args, **kwargs)
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context):
        try:
            table_files = glob.glob(self.folder_path + '/*.json')
            for table in table_files:
                with open(table, 'r') as f:
                    table_data = f.read()
                    # Header and submit to pinot
                    headers = {'Content-Type':'application/json'}
                    response = requests.post(self.pinot_url,headers=headers,data=table_data)
                    if response.status_code == 200:
                        self.log.info(f'Table successfully submitted to Pinot : {table}')
                    else:
                        self.log.error(f'An error occurred while submitting tables {response.status_code - response.text}')
                        raise Exception('table submition failed')
        except Exception as e:
            self.log.error(f'An error occurred while executing : {str(e)}')