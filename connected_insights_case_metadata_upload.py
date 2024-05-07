from pprint import pprint
import requests
from requests.structures import CaseInsensitiveDict
import json
import os
import argparse
import base64
import sys
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
def get_workgroup_id(domain_url,auth_credentials,workgroup_name=None):
    endpoint = "/crs/api/v1/session/workgroups"
    full_url = domain_url + endpoint
    headers = CaseInsensitiveDict()
    headers['accept'] = "application/json"
    headers['X-ILMN-Domain'] = auth_credentials['X-ILMN-Domain']
    headers['Authorization'] = auth_credentials['Authorization']
    headers['Content-Type'] = "application/json"
    workgroup_id = None
    try:
        connected_insights_response = requests.get(full_url, headers=headers)
        ### pick first workgroup observed if no workgroup name figben
        if workgroup_name is None:
            workgroup_roles = connected_insights_response.json()['workgroupRoles'][0]
            workgroup_id = workgroup_roles['orgid']
            workgroup_name = workgroup_roles['orgName']
        else:
            for idx,workgroup in enumerate(connected_insights_response.json()['workgroupRoles']):
                if workgroup['orgName'] == workgroup_name:
                    workgroup_id = workgroup['orgid']
                    workgroup_name = workgroup['orgName'] 
        print(f"Found workgroup id {workgroup_id} associated to the workgroup name {workgroup_name}")
    except:
        pprint(platform_response,indent=4)
        raise ValueError(f"Could not find workgroup id for user for the following URL: {domain_name}")
    return workgroup_id

# Validation STEP 2A: Check on Tumor Type --- SNOWMEDCT IDs configured in Connected Insights 
def get_diseases_configured(domain_url,auth_credentials):
    valid_snowmedct_ids = dict()
    endpoint_url = f"/cfg/api/v1/disease-config"
    full_url = domain_url + endpoint_url
    headers = CaseInsensitiveDict()
    headers['accept'] = 'application/json'
    headers['Content-Type'] = 'application/json'
    headers['X-ILMN-Domain'] = auth_credentials['X-ILMN-Domain']
    headers['Authorization'] = auth_credentials['Authorization']
    headers['X-ILMN-Workgroup'] = auth_credentials['X-ILMN-Workgroup']
    try:
        diseases_configured = requests.get(full_url, headers=headers)
        diseases_configured_items = json.loads(diseases_configured.text)
        for idx,item in enumerate(diseases_configured_items):
            if 'associatedDiseasesTerms' in list(item.keys()):
                if item['associatedDiseasesTerms'] is not None:
                    for idx,term in enumerate(item['associatedDiseasesTerms']):
                        if 'externalId' in list(term.keys()) and 'synonym' in list(term.keys()):
                            synonym_string = ", ".join(term['synonym'])
                            valid_snowmedct_ids[str(term['externalId'])] = f"{synonym_string}"
    except:
        pprint(json.loads(diseases_configured.text),indent=4)
        raise ValueError(f"Could not get diseases configured for {domain_url}")
    return valid_snowmedct_ids

def validate_tumor_types_in_csv(ids_of_interest,metadata_csv):
    validation_object = dict()
    with open(metadata_csv,"r") as open_file:
        for line_num,line in enumerate(open_file.readlines()):
            line_cleaned = line.rstrip()
            line_split = line_cleaned.split(',')
            line_split = [str(x) for x in line_split]
            if 'Tumor_Type' in line_split:
                for idx,field in enumerate(line_split):
                    if field == "Tumor_Type":
                        field_num_of_interest = idx
            else:
                # Create warning message if provided SNOWMEDCT ID is not found in Connected Insights workgroup configuration
                if line_split[field_num_of_interest] not in ids_of_interest:
                    ids_of_interest_str = ", ".join(ids_of_interest)
                    warning_str = f"[Warning] Could not find a valid Tumor_Type in line [Line number {str(line_num)}] {line}\nFound the following id: {line_split[field_num_of_interest]}\nExpected one of these SNOWMEDCT IDs: {ids_of_interest_str}\n"
                    validation_object[f"Line {str(line_num)}"] = warning_str
    return validation_object

# Validation STEP 2B: Check if cases in CSV intersect with Case_IDs present in in Connected Insights 
def get_cases_present(domain_url,auth_credentials):
    current_cases = dict()
    page_size = 1000
    pages_parsed = 0
    endpoint_url = f"/crs/api/v1/cases/search?pageNumber={pages_parsed}&pageSize={page_size}"
    full_url = domain_url + endpoint_url
    headers = CaseInsensitiveDict()
    headers['accept'] = '*/*'
    headers['Content-Type'] = 'application/json'
    headers['X-ILMN-Domain'] = auth_credentials['X-ILMN-Domain']
    headers['Authorization'] = auth_credentials['Authorization']
    headers['X-ILMN-Workgroup'] = auth_credentials['X-ILMN-Workgroup']
    try:
        cases_present = requests.get(full_url, headers=headers)
        cases_present_items = json.loads(cases_present.text)
        total_cases = cases_present_items['totalElements']
        while total_cases > (pages_parsed * page_size):
            #print(f"hi {cases_present_items['totalElements']}")
            for idx,case in enumerate(cases_present_items['content']):
                current_cases[case['displayId']] = case['status']
            pages_parsed = pages_parsed + 1
            endpoint_url = f"/crs/api/v1/cases/search?pageNumber={pages_parsed}&pageSize={page_size}"
            full_url = domain_url + endpoint_url
            cases_present = requests.get(full_url, headers=headers)
            cases_present_items = json.loads(cases_present.text)
            #print(f"done {pages_parsed * page_size}")
    except:
        pprint(json.loads(cases_present.text),indent = 4)
        raise ValueError(f"Could not get cases present for {domain_url}")
    return current_cases

def validate_case_id_in_csv(ids_of_interest,metadata_csv):
    validation_object = dict()
    with open(metadata_csv,"r") as open_file:
        for line_num,line in enumerate(open_file.readlines()):
            line_cleaned = line.rstrip()
            line_split = line_cleaned.split(',')
            line_split = [str(x) for x in line_split]
            if 'Case_ID' in line_split:
                for idx,field in enumerate(line_split):
                    if field == "Case_ID":
                        field_num_of_interest = idx
            else:
                # Create warning message if provided Case ID is already found in this Connected Insights workgroup
                if line_split[field_num_of_interest] in ids_of_interest:
                    ids_of_interest_str = ", ".join(ids_of_interest)
                    warning_str = f"[Warning] Found Case_ID in line [Line number {str(line_num)}] {line}\nFound the following id: {line_split[field_num_of_interest]}\nThis may impact the ingestion of this case\n"
                    validation_object[f"Line {str(line_num)}"] = warning_str
    return validation_object

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
        #print(file_id)
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
    parser.add_argument('--workgroup_id', default=None, type=str, help="[OPTIONAL] Connected Insights Workgroup ID")
    parser.add_argument('--workgroup_name', default=None, type=str, help="[OPTIONAL] Connected Insights Workgroup Name to grab Workgroup ID")
    parser.add_argument('--application_name', default="connectedinsights", type=str, help="Connected Insights app name alias. Most usecases will not need this to be configured.")
    parser.add_argument('--platform_url', default="https://platform.login.illumina.com", type=str, help="Illumina Platform authentication.Most usecases will not need this to be configured.")
    parser.add_argument('--lenient_mode',  action="store_true", help="lenient mode crafting CSV for debugging")
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
    print(f"Grabbing user metadata for {domain_url}")
    ps_token = generate_ps_token(platform_url,application_name,domain_url,encoded_key)
    auth_credentials['Authorization'] = f"{ps_token}"
    auth_credentials['X-ILMN-Domain'] = domain_url.strip("https://").split(".")[0]

    # STEP 2: Obtain WorkgroupID
    if args.workgroup_name is None and args.workgroup_id is None:
        workgroup_id = get_workgroup_id(domain_url,auth_credentials)
    elif args.workgroup_name is not None and args.workgroup_id is None :
        workgroup_id = get_workgroup_id(domain_url,auth_credentials,args.workgroup_name)
    elif args.workgroup_name is None and args.workgroup_id is not None:
        workgroup_id = args.workgroup_id
    elif args.workgroup_name is not None and workgroup_id is not None:
        workgroup_id = get_workgroup_id(domain_url,auth_credentials,args.workgroup_name)
    if workgroup_id is None:
        raise ValueError(f"Could not find workgroup id in the domain {domain_url}")
    auth_credentials['X-ILMN-Workgroup'] = workgroup_id
    
    # Validation STEP 2A: Check on Tumor Type --- SNOWMEDCT IDs configured in Connected Insights 
    print(f"Validating Tumor_Type in Case Metadata file {metadata_csv}")
    configured_disease_terms = get_diseases_configured(domain_url,auth_credentials)
    configured_snowmedct_ids = list(configured_disease_terms.keys())
    invalid_tumor_type_configurations = validate_tumor_types_in_csv(configured_snowmedct_ids,metadata_csv)
    if len(list(invalid_tumor_type_configurations.keys())) > 0:
        for idx,validation_warning in enumerate(invalid_tumor_type_configurations):
            print(invalid_tumor_type_configurations[validation_warning])
    if args.lenient_mode is False:
        raise ValueError(f"Invalid Tumor Type(s) supplied to {metadata_csv}")
    
    print(f"Validating Case_IDs in Case Metadata file {metadata_csv}")
    # Validation STEP 2B: Check if cases in CSV intersect with Case_IDs present in in Connected Insights 
    cases_present_in_ici_metadata = get_cases_present(domain_url,auth_credentials)
    cases_present_in_ici = list(cases_present_in_ici_metadata.keys())
    ##print(cases_present_in_ici)
    case_id_warnings = validate_case_id_in_csv(cases_present_in_ici,metadata_csv)
    if len(list(case_id_warnings.keys())) > 0:
        for idx,validation_warning in enumerate(case_id_warnings):
            print(case_id_warnings[validation_warning])
    if args.lenient_mode is False:
        raise ValueError(f"Case ID already found in Connected Insights.\nEither modify {metadata_csv} with unique case identifiers or delete case and re-ingest metadata")


    # STEP 3: Upload Case Metadata into Connected Insights
    print(f"Uploading Case Metadata {metadata_csv} to Connected Insights")
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