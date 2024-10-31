import requests
import logging
import pandas as pd

logger = logging.getLogger('__main__.' + __name__)

class ChefsApi:
    base_url = "https://submit.digital.gov.bc.ca/app/api/v1"

    def __init__(self, api_key_secret, form_id, form_version_id):
        """
        api_key_secret: the API key secret for the CHEFS form
        form_id: the form ID for the CHEFS form
        form_version_id: the form version ID for the CHEFS form
        """
        self.api_key_secret = api_key_secret
        self.form_id = form_id
        self.form_version_id = form_version_id

        # endpoints
        self.forms_endpoint = f'/forms'
        self.form_endpoint = f'/forms/{self.form_id}'
        self.published_version_endpoint = f'/forms/{self.form_id}/version'
        self.options_endpoint = f'/forms/{self.form_id}/options'
        self.export_endpoint = f'/forms/{self.form_id}/export'
        self.status_codes_endpoint = f'/forms/{self.form_id}/statusCodes'
        self.fields_endpoint = f'/forms/{self.form_id}/versions/{self.form_version_id}/fields'
        self.submissions_endpoint = f'/forms/{self.form_id}/versions/{self.form_version_id}/submissions'
        self.version_submissions_endpoint = f'/forms/{self.form_id}/versions/{self.form_version_id}/submissions'

    def generate_basic_auth_token(self):
        """Generates the BasicAuth token for the API"""
        basic_auth_token = requests.auth._basic_auth_str(username=self.form_id, password=self.api_key_secret)
        return basic_auth_token

    def get(self, endpoint: str):
        headers = {
            "Content-Type": "application/json",
            "Authorization": self.generate_basic_auth_token(),
            "Connection": "keep-alive",
        }

        response = requests.get(
            url=self.base_url + endpoint,
            headers=headers,
            # params=params,
            verify=True,
            timeout=1800,
        )

        if response.status_code == 200:
            return response
        else:
            raise requests.exceptions.HTTPError(response.status_code)

    def get_json(self, endpoint: str) -> dict:
        response = self.get(endpoint)
        return response.json()
    
    def get_current_status(self, submission_id):
        """
        Returns the status of submission_id
        """
        status_endpoint = f'/submissions/{submission_id}/status'
        data = self.get_json(status_endpoint)
        df = pd.DataFrame.from_records(data) 
        curr_status = df.sort_values(by='updatedAt', ascending=False)['code'].iloc[0]
        return curr_status
