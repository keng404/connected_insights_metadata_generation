from pprint import pprint
import requests
from requests.structures import CaseInsensitiveDict
import json
import os
import argparse
import base64
# STEP 1: generate psToken from username and password
def generate_ps_token(platform_url,application_name,domain_url,credentials):
    platform_services_url = f"{platform_url}/platform-services-manager/Session/"
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['grant_type'] = "password"
    headers['Authorization'] = f"Basic {credentials}"
    headers['Content-Type'] = "application/json"
    data = {
    'clientId': f"{application_name}",
    'rURL': f"{domain_url}"
    }
    access_token = None
    try:
        platform_response = requests.post(platform_services_url, headers=headers,data=json.dumps(data))
        access_token = platform_response.json()['access_token']
    except:
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not generate psToken for the following URL: {domain_url}")
    return access_token    

# STEP 2: obtain WorkgroupID
def get_workgroup_id(domain_url,auth_credentials):
    endpoint = "/crs/api/v1/session/workgroups"
    full_url = domain_url + endpoint
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['X-ILMN-Domain'] = auth_credentials['X-ILMN-Domain']
    headers['Authorization'] = auth_credentials['Authorization']
    headers['Content-Type'] = "application/json"
    try:
        connected_insights_response = requests.get(full_url, headers=headers)
        workgroup_roles = connected_insights_response.json()['workgroupRoles'][0]
        workgroup_id = workgroup_roles['orgid']
        workgroup_name = workgroup_roles['orgName']
        print(f"Found workgroup id {workgroup_id} associated to the workgroup name {workgroup_name}")
    except:
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find workgroup id for user for the following URL: {domain_name}")
    return workgroup_id

# STEP 3: Upload Case Metadata into Connected Insights
def upload_case_metadata(domain_url,auth_credentials,metadata_csv):
    endpoint_url = f"/crs/api/v2/custom-case-data/files"
    full_url = domain_url + endpoint_url
    headers = CaseInsensitiveDict()
    headers['accept'] = "*/*"
    headers['X-ILMN-Domain'] = auth_credentials['X-ILMN-Domain']
    headers['X-ILMN-Workgroup'] = auth_credentials['X-ILMN-Workgroup']
    headers['Authorization'] = auth_credentials['Authorization']
    input_file = os.path.basename(f"{metadata_csv}")
    files = {'files' : (f"{input_file}", open(f"{metadata_csv}", 'rb'), 'text/csv') }
    metadata_csv_ingestion_response = None
    file_id = None
    try:
        metadata_csv_ingestion_response = requests.post(full_url, headers=headers,files=files)
        #pprint(metadata_csv_ingestion_response.text,indent=4)
        file_id = json.loads(metadata_csv_ingestion_response.text)[0]['id']
        #pprint(metadata_csv_ingestion_response.text,indent=4)
        print(file_id)
    except:
        pprint(metadata_csv_ingestion_response,indent=4)
        raise ValueError(f"Could not upload {metadata_csv}")
    return file_id

# STEP 4: Check on ingestion status
def case_metadata_ingestion_check(domain_url,auth_credentials,file_id):
    endpoint_url = f"/crs/api/v1/custom-case-data/{file_id}/status"
    full_url = domain_url + endpoint_url
    headers = CaseInsensitiveDict()
    headers['accept'] = "*/*"
    headers['X-ILMN-Domain'] = auth_credentials['X-ILMN-Domain']
    headers['X-ILMN-Workgroup'] = auth_credentials['X-ILMN-Workgroup']
    headers['Authorization'] = auth_credentials['Authorization']
    ingestion_status = None
    try:
        ingestion_status = requests.get(full_url, headers=headers)
        pprint(ingestion_status.text,indent=4)
    except:
        pprint(ingestion_status,indent=4)
        raise ValueError(f"Could not ingest {ingestion_status['source']}")
    return json.loads(ingestion_status.text)
#################################
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--domain_url', default=None,required=True, type=str, help="Connected Insights domain URL")
    parser.add_argument('--username', default=None,required=True, type=str, help="username [email] used to log into Connected Insights")
    parser.add_argument('--password', default=None,required=True, type=str, help="password used to log into Connected Insights")
    parser.add_argument('--metadata_csv', default=None,required=True, type=str, help="output CSV containing case metadata for Connected Insights")
    parser.add_argument('--application_name', default="connectedinsights", type=str, help="Connected Insights app name alias. Most usecases will not need this to be configured.")
    parser.add_argument('--platform_url', default="https://platform.login.illumina.com", type=str, help="Illumina Platform authentication.Most usecases will not need this to be configured.")
    args, extras = parser.parse_known_args()
    #############
    username = args.username
    password = args.password
    metadata_csv = args.metadata_csv
    domain_url = args.domain_url
    application_name = args.application_name
    platform_url = args.platform_url
    ############

    ## base64 encode username password combination
    encoded_key = base64.b64encode(bytes(f"{username}:{password}", "utf-8")).decode()
    auth_credentials = dict()

    # STEP 1: Generate psToken from username and password
    ps_token = generate_ps_token(platform_url,application_name,domain_url,encoded_key)
    auth_credentials['Authorization'] = f"{ps_token}"
    auth_credentials['X-ILMN-Domain'] = domain_url.strip("https://").split(".")[0]

    # STEP 2: Obtain WorkgroupID
    workgroup_id = get_workgroup_id(domain_url,auth_credentials)
    auth_credentials['X-ILMN-Workgroup'] = workgroup_id

    # STEP 3: Upload Case Metadata into Connected Insights
    file_id = upload_case_metadata(domain_url,auth_credentials,metadata_csv)

    # STEP 4: Check on ingestion status and report back
    ingestion_status = "QUEUED"
    while ingestion_status in ["QUEUED","IN_PROGRESS"]:
        ingestion_metadata = case_metadata_ingestion_check(domain_url,auth_credentials,file_id)
        ingestion_status = ingestion_metadata['status']

    ### TODO
    # How to deal with users with multiple workgroups?
    # Check if Case exists before ingestion?
0
#################
if __name__ == '__main__':
    main()