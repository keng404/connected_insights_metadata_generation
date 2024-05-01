# Helper modules to interact with ICA Base
import snowflake.connector
from pprint import pprint
import requests
from requests.structures import CaseInsensitiveDict
import pandas as pd
import re
import os
import argparse
import json
from datetime import datetime as dt

### check if API KEY is valid 
def validate_api_key(api_key):
    valid_api_key = False
    api_base_url = os.environ['ICA_BASE_URL'] +"/ica/rest"
    endpoint = f"/api/tokens"
    full_url = api_base_url + endpoint
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        token_response = requests.get(full_url, headers=headers)
        valid_api_key = True
    except:
        pprint(token_response,indent=4)
        raise ValueError(f"Could not validate the API_KEY: {api_key}")
    return(valid_api_key)

### check if PROJECT_ID is valid, do we allow by PROJECT_NAME as well?
def valid_project_id(api_key,project_id):
    valid_project_id = False
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}"
    full_url = api_base_url + endpoint
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        project_response = requests.get(full_url, headers=headers)
        valid_project_id = True
    except:
        pprint(project_response,indent=4)
        raise ValueError(f"Could not get find project with the identifier: {project_id}")
    return(valid_project_id)

### use if project name is provided and project id is not
def get_project_id(api_key, project_name):
    projects = []
    pageOffset = 0
    pageSize = 30
    page_number = 0
    number_of_rows_to_skip = 0
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects?search={project_name}&includeHiddenProjects=true&pageOffset={pageOffset}&pageSize={pageSize}"
    full_url = api_base_url + endpoint  ############ create header
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        projectPagedList = requests.get(full_url, headers=headers)
        totalRecords = projectPagedList.json()['totalItemCount']
        while page_number * pageSize < totalRecords:
            projectPagedList = requests.get(full_url, headers=headers)
            for project in projectPagedList.json()['items']:
                projects.append({"name": project['name'], "id": project['id']})
            page_number += 1
            number_of_rows_to_skip = page_number * pageSize
    except:
        raise ValueError(f"Could not get project_id for project: {project_name}")
    if len(projects) > 1:
        pprint(projects,indent = 4)
        raise ValueError(f"There are multiple projects that match {project_name}")
    else:
        return projects[0]['id']
############

def get_ica_base_connection(api_key,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/base:connectionDetails"
    #### BOILERPLATE header JSON for most GET and POST requests via the API
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    ###### full URL is the the base url (ICA base rest URL)  plus the endpoint (relative route that is displayed in our SWAGGER page)
    full_url = api_base_url + endpoint
    ####### POST request to get ICA Base connection metadata
    try:
        ICA_base_connection_details = requests.post(full_url, headers=headers)
        ICA_base_connection_details = ICA_base_connection_details.json()
    except:
        pprint(ICA_base_connection_details,indent = 4)
        raise ValueError(f"Do you have ICA Base enabled in your project?\n\nDo you have any Base Tables in your project?\n\nCould not get ICA Base connection details for project: {project_name}")
    return(ICA_base_connection_details)
###

### List Base tables see if any have Clarity in them?
def get_base_tables(api_key,project_id):
    api_base_url = os.environ['ICA_BASE_URL'] + "/ica/rest"
    endpoint = f"/api/projects/{project_id}/base/tables"
    full_url = api_base_url + endpoint
    #### BOILERPLATE header JSON for most GET and POST requests via the API
    headers = CaseInsensitiveDict()
    headers['Accept'] = 'application/vnd.illumina.v3+json'
    headers['Content-Type'] = 'application/vnd.illumina.v3+json'
    headers['X-API-Key'] = api_key
    try:
        tables_response = requests.get(full_url, headers=headers)
    except:
        pprint(tables_response,indent=4)
        raise ValueError(f"Could not get ICA Base tables for the project {project_id}")
    return(tables_response.json()['items'])

def base_table_sanity_check(ica_base_table_metadata):
    sanity_check_passed = True
    base_table_names = []
    for idx,t in enumerate(ica_base_table_metadata):
        base_table_names.append(t['name'])
    ### check if you have any Base tables
    if len(base_table_names) < 1:
        sanity_check_passed = False
        raise ValueError(f"Do you have any Base tables in the project {project_id}?")
    ### check if you have any clarity tables:
    clarity_table_count = 0
    for n in base_table_names:
        if re.match("CLARITY",n) is not None:
            clarity_table_count = clarity_table_count + 1
    if clarity_table_count == 0:
        sanity_check_passed = False
        table_name_str = ", ".join(base_table_names)
        print("These are the Table names in your project:\n\n")
        print(table_name_str)
        raise ValueError(f"Did you enable the Data Catologue in your ICA Domain?\n\nSee https://help.ica.illumina.com/project/p-base/base-tables/datacatalogue")
    return sanity_check_passed


def connect_to_snowflake(ICA_base_connection_details):
    ctx = snowflake.connector.connect(
        authenticator=ICA_base_connection_details['authenticator'],
        token=ICA_base_connection_details['accessToken'], 
        database=ICA_base_connection_details['databaseName'],
        role=ICA_base_connection_details['roleName'],
        warehouse=ICA_base_connection_details['warehouseName'],
        account=ICA_base_connection_details['dnsName'].split('.snowflakecomputing.com')[0],
        schema=ICA_base_connection_details['schemaName']
    )
    return ctx

#print(f"Connecting to Snowflake warehouse")
#snowflake_connector_object.cursor().execute(f"USE {ICA_base_connection_details['databaseName']}")


############# Default CLARITY table we are interested in from ICA Base
#base_table_of_interest = "CLARITY_SAMPLE_VIEW_tenant"
### TODO :: add potential filter by CREATE_TIME to limit number of records returned as number of samples grows?
### Specify query on fields that are of interest? For Troubleshooting (Connected Insights,Clarity, ICA)?
def load_clarity_sample_table(snowflake_connector_object = None,  base_table_of_interest = None):
    if snowflake_connector_object is None:
        raise ValueError(f"Please provide a snowflake connection object\nUse the functions get_ica_base_connection and connect_to_snowflake")
    df = None
    if base_table_of_interest is None:
        base_table_of_interest = "CLARITY_SAMPLE_VIEW_tenant"
    base_query = f"SELECT * FROM {base_table_of_interest}"
    print("SQL Query \n\n")
    pprint(base_query)
    base_results = None
    cur = snowflake_connector_object.cursor()
    #### try finally block can probably be removed/simplified
    try:
        cur.execute_async(base_query)
        query_id = cur.sfqid
        cur.get_results_from_sfqid(query_id)
        base_results = cur.fetch_pandas_all()
        df = base_results
    finally:
        cur.close()    
    return df

#print(df.head())

### check if nothing is returned --- valid sample id or is this the right table name?
def subset_clarity_sample_view(clarity_sample_data = None, sample_ids = [], clarity_lims_sample_project = None):
    column_of_interest = "DATA"
    results = []
    sample_id_count = {}
    if clarity_sample_data is None:
        raise ValueError("Please provide results from Clarity")
    if len(sample_ids) < 1 and clarity_lims_sample_project is None:
        raise ValueError("Please provide a sample identifier to query on OR a Clarity LIMS sample project to query on")
    if  len(sample_ids) > 0:
        for s in sample_ids:
            sample_id_count[s] = 0
    
    ## parse this column, it will be a JSON
    ## Will convert JSON string into python object
    sample_metadata = [json.loads(d) for d in clarity_sample_data[column_of_interest]]
    for record in sample_metadata:
        if len(sample_ids) > 0 and clarity_lims_sample_project is None:
            if record['id'] in sample_ids:
                results.append(record)
                if record['id'] in sample_id_count.keys():
                    sample_id_count[record['id']] = sample_id_count[record['id']] + 1
        elif len(sample_ids) < 1 and clarity_lims_sample_project is not None:
            if record['limsSampleProject'] == clarity_lims_sample_project:
                results.append(record)
                if record['id'] in sample_id_count.keys():
                    sample_id_count[record['id']] = sample_id_count[record['id']] + 1
                else:
                    sample_id_count[record['id']] = 1
        elif  len(sample_ids) > 0 and clarity_lims_sample_project is not None:
            if record['limsSampleProject'] == clarity_lims_sample_project and rorecordw['id'] in sample_ids:
                results.append(record)
                if record['id'] in sample_id_count.keys():
                    sample_id_count[record['id']] = sample_id_count[record['id']] + 1
                else:
                    sample_id_count[record['id']] = 1

    # check for each sample if we've gotten any results, multiple results
    for sample_id in list(sample_id_count.keys()):
        number_of_results = sample_id_count[sample_id]
        if number_of_results == 0:
            raise ValueError(f"Could not find any results for {sample_id}")
        if number_of_results > 1:
            pprint(results,indent=4)
            print(f"[WARNING] Found multiple results for {sample_id}")
    # or no results
    if len(results) < 1:
        error_string = f"Could not find any matches for"
        if len(sample_ids) > 0:
            sample_id_str = ", ".join(sample_ids)
            error_string = error_string + f" {sample_ids} "
        if clarity_lims_sample_project is not None:
            error_string = error_string + f" in the Clarity LIMS Sample Project {clarity_lims_sample_project} "
        raise ValueError(f"{error_string}")
    return results

### check if sample has metadata fields we are looking for
# in case Clarity stores info differently from the fields we are interested in
field_map_dict = dict()
field_map_dict["Sample_ID"] = "id" 
mandatory_fields = ["Sample_ID", "Tumor_Type", "Case_ID"]
### TODO: may need to query Connected Insights to grab custom fields associated to Test_Definition/workflow
### these are optional for now --- cases ingested via this script
### custom fields can be mandatory for case ingestion
### 3.0 and 4.0 have different API routes
other_fields_of_interest = ["Sample_Type","Sample_Classification","Tags","Test_Definition","Sample Name(s)"]
fields_ignore = ["container"]
def parse_table_row(row):
    row_mandatory_fields = dict()
    row_optional_fields = dict()
    for idx,field in enumerate(row):
        if field not in fields_ignore:
            if field == "userDefinedFields":
                for i,x in enumerate(row["userDefinedFields"]):
                    if x["key"] in mandatory_fields:
                        for k in mandatory_fields:
                            if x["key"] == k:
                                row_mandatory_fields[k] = x["value"]
                    elif x["key"] in other_fields_of_interest:
                        for k in other_fields_of_interest:
                            if x["key"] == k:
                                row_optional_fields[k] = x["value"]
                    elif x["key"] in list(field_map_dict.values()):
                        for i,k in enumerate(field_map_dict):
                            if x["key"] == field_map_dict[k] and k in mandatory_fields:
                                row_mandatory_fields[k] = x["value"]
                            elif x["key"] == field_map_dict[k] and k in other_fields_of_interest:
                                row_optional_fields[k] = x["value"]
            else:
                if field in mandatory_fields:
                    for k in mandatory_fields:
                        if field == k:
                            row_mandatory_fields[k] = row[field]
                elif field in other_fields_of_interest:
                    for k in other_fields_of_interest:
                        if field == k:
                            row_optional_fields[k] = row[field]
                elif field in list(field_map_dict.values()):
                    for i,k in enumerate(field_map_dict):
                        if field == field_map_dict[k] and k in mandatory_fields:
                            row_mandatory_fields[k] = row[field]
                        elif field == field_map_dict[k] and k in other_fields_of_interest:
                            row_optional_fields[k] = row[field]
    return (row_mandatory_fields,row_optional_fields)


############################################
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--project_id', default=None, type=str, help="ICA project id as seen in Base")
    parser.add_argument('--project_name', default=None, type=str, help="ICA project name")
    parser.add_argument('--sample_id', nargs='+', type=str, help="Sample Identifier to query from Clarity")
    parser.add_argument('--lims_sample_project', default=None, type=str, help="Clarity LIMS Sample project to query on")
    parser.add_argument('--output_csv', default=None, type=str, help="output CSV containing case metadata for Connected Insights")
    parser.add_argument('--ica_base_url', default="https://ica.illumina.com", type=str, help="ICA base url. In most use-cases, this option does not need to be configured")
    parser.add_argument('--api_key', default=None, type=str, help="A string that is the API Key")
    parser.add_argument('--api_key_file', default=None, type=str, help="file that contains API Key")
    parser.add_argument('--lenient_mode',  action="store_true", help="lenient mode crafting CSV for debugging")
    args, extras = parser.parse_known_args()
    #############
    #######################
    PROJECT_ID = None
    API_KEY = None
    SAMPLE_ID = []
    PROJECT_NAME = None
    LIMS_SAMPLE_PROJECT = None
    OUTPUT_CSV = None
    ################
    os.environ['ICA_BASE_URL'] = args.ica_base_url

    if args.output_csv is not None:
        OUTPUT_CSV = args.output_csv
    else:
        dateTimeObj = dt.now()
        timestampStr = dateTimeObj.strftime("%Y%b%d_%H_%M_%S_%f")
        OUTPUT_CSV = f"case_metadata.connected_insights.{timestampStr}.csv"
    
    # Argument checks for API KEY
    if args.api_key is None and args.api_key_file is None:
        raise ValueError("Please provide either a project id (--api_key) or project name (--api_key_file)")
    elif args.api_key is None and args.api_key_file is not None:
        if os.path.isfile(args.api_key_file) is True:
            with open(args.api_key_file, 'r') as f:
                API_KEY = str(f.read().strip("\n"))
    else:
        API_KEY = args.api_key
    ## Validate API_KEY    
    if validate_api_key(API_KEY) is True:
        print(f"[Pre-Flight-Check] Authentication via API KEY passed on ICA")

    # Argument checks for PROJECT ID/NAME
    if args.project_id is None and args.project_name is None:
        raise ValueError("Please provide either a project id (--project_id) or project name (--project_name)")
    elif args.project_id is None and args.project_name is not None:
        PROJECT_ID = get_project_id(API_KEY, PROJECT_NAME)
    else:
        PROJECT_ID = args.project_id
    # Check if Project is valid
    if valid_project_id(API_KEY,PROJECT_ID) is True:
        print(f"[Pre-Flight-Check] ICA Project is valid and accessible to user")

    # Argument checks for Samples/Projects we'll generate a metadata samplesheet for case ingestion into Connected Insights
    if len(args.sample_id) < 1 and args.lims_sample_project is None:
        raise ValueError("Please provide either a list of sample ids to query (--sample_id sample1 sample2 ... sampleN) or the LIMS project (--lims_sample_project) ")
    if len(args.sample_id) > 0:
        SAMPLE_ID = args.sample_id
        sample_id_str = ", ".join(SAMPLE_ID)
        print(f"[Pre-Flight-Check] Generating metadata for Connected Insights using these samples: {sample_id_str}")
    if args.lims_sample_project is not None:
        LIMS_SAMPLE_PROJECT = args.lims_sample_project
        print(f"[Pre-Flight-Check] Generating metadata for Connected Insights using samples from this project in Clarity LIMS: {LIMS_SAMPLE_PROJECT}")

    # STEP 1: Get ICA base connection details
    print(f"STEP 1: Get ICA base connection details")
    ica_base_table_metadata = get_base_tables(API_KEY,PROJECT_ID)
    if base_table_sanity_check(ica_base_table_metadata) is True:
        ICA_base_connection_details = get_ica_base_connection(API_KEY,PROJECT_ID)
    else:
        raise ValueError(f"Does your project {PROJECT_ID} have Base tables from the Data Catalogue in them?")

    # STEP 2: Create Snowflake connector object and connect to Warehouse
    snowflake_connector_object = connect_to_snowflake(ICA_base_connection_details)
    print(f"STEP 2: Connecting to Snowflake Warehouse {ICA_base_connection_details['databaseName']}")
    snowflake_connector_object.cursor().execute(f"USE {ICA_base_connection_details['databaseName']}")

    # STEP 3: Query Clarity_SAMPLE_VIEW_tenant table
    print(f"STEP 3: Loading data from Base table Clarity_SAMPLE_VIEW_tenant")
    clarity_sample_data = load_clarity_sample_table(snowflake_connector_object = snowflake_connector_object,  base_table_of_interest = "Clarity_SAMPLE_VIEW_tenant")

    # STEP 4: Subset view by sample id(s) or by LIMS_SAMPLE_PROJECT
    print(f"STEP 4: Subsetting Sample metadata by sample ID or Clarity LIMS project name")
    if len(SAMPLE_ID) < 1 and LIMS_SAMPLE_PROJECT is not None:
        subset_clarity_sample_data = subset_clarity_sample_view(clarity_sample_data = clarity_sample_data, sample_ids = [], clarity_lims_sample_project = LIMS_SAMPLE_PROJECT)
    elif len(SAMPLE_ID) > 0 and LIMS_SAMPLE_PROJECT is None:
        subset_clarity_sample_data = subset_clarity_sample_view(clarity_sample_data = clarity_sample_data, sample_ids = SAMPLE_ID, clarity_lims_sample_project = None)
    elif len(SAMPLE_ID) > 0 and LIMS_SAMPLE_PROJECT is not None:
        subset_clarity_sample_data = subset_clarity_sample_view(clarity_sample_data = clarity_sample_data, sample_ids = SAMPLE_ID, clarity_lims_sample_project = LIMS_SAMPLE_PROJECT)


    # STEP 5: Sanity check we have all mandatory fields for ingestion ;  warning for missing (optional + custom) fields
    print(f"STEP 5: Checking Sample data of interest to see if we have data of interest")
    # First pass --- collect info
    initial_data_for_metadata_csv = []
    table_mandatory_fields = []
    table_optional_fields = []
    for idx,r in enumerate(subset_clarity_sample_data):
        parsed_objects = parse_table_row(r)
        table_mandatory_fields = table_mandatory_fields + list(parsed_objects[0].keys())
        table_optional_fields = table_optional_fields + list(parsed_objects[1].keys())
        initial_data_for_metadata_csv.append(list(parsed_objects))

    # second pass form lines based on minimal set of info mandatory fields and union of optional_fields --- if optional fields is empty, ignore
    mandatory_fields_found = list(set(table_mandatory_fields))
    optional_fields_found = list(set(table_optional_fields))
    if len(mandatory_fields_found) == 0 and len(optional_fields_found) == 0:
        raise ValueError(f"Could not find any fields on interest")
    elif len(mandatory_fields_found) == 0:    
        mandatory_fields_str = ", ".join(mandatory_fields)
        raise ValueError(f"Could not find any of the mandatory fields on interest {mandatory_fields_str}")
    
    warning_lines = 0
    final_data_for_metadata_csv = []
    if len(optional_fields_found) > 0 :
        all_headers = mandatory_fields + optional_fields_found
    else:
        all_headers = mandatory_fields
    all_headers_str = ",".join(all_headers)
    final_data_for_metadata_csv.append(all_headers_str)
    for r in initial_data_for_metadata_csv:
        final_line = []
        missing_mandatory_fields = []
        missing_mandatory_flag = 0
        missing_optional_fields = []
        missing_optional_flag = 0
        #### mandatory fields for row
        if len(r[0]) > 0 :
            for mandatory in mandatory_fields:
                if mandatory in r[0].keys():
                    final_line.append(r[0][mandatory])
                else:
                    missing_mandatory_flag = missing_mandatory_flag + 1
                    final_line.append("")
                    missing_mandatory_fields.append(mandatory)
        else:
            for mandatory in mandatory_fields:
                final_line.append("")
                missing_mandatory_fields.append(mandatory)
         #### optional fields for row
        if len(optional_fields_found) > 0:
            if len(r[1]) > 0:
                for optional in optional_fields_found:
                    if optional in r[1].keys():
                        final_line.append(r[1][optional])
                    else:
                        missing_optional_fields = missing_optional_fields + 1
                        final_line.append("")
                        missing_optional_fields.append(optional)
            else:
                for optional in optional_fields:
                    final_line.append("")
                    missing_optional_fields.append(optional)       

        # final line
        line_str = ",".join(final_line) 
        final_data_for_metadata_csv.append(line_str)

        # print out warnings
        if missing_optional_flag > 0 or missing_mandatory_flag > 0:
            warning_lines = warning_lines + 1
        if len(missing_mandatory_fields) > 0:
            missing_mandatory_fields_str = ",".join(missing_mandatory_fields)
            print(f"[Warning] Missing Mandatory fields {missing_mandatory_fields_str} in  line {line_str}")
        if len(missing_optional_fields) > 0:
            missing_optional_fields_str = ",".join(missing_optional_fields)
            print(f"[Warning] Missing Optional fields {missing_optional_fields_str} in  line {line_str}")

    # STEP 6: Generate metadata CSV
    print(f"STEP 6: Creating metadata CSV for ingestion into Connected Insights")
    all_lines = "\n".join(final_data_for_metadata_csv)
    with open(f"{OUTPUT_CSV}", "w") as outfile:
        outfile.write(f"{all_lines}")

    if args.lenient_mode is True:
        print(f"There are {warning_lines} to fix in the file {OUTPUT_CSV}.\nSee warnings above.")
    else:
        if warning_lines > 0:
            raise ValueError(f"There are {warning_lines} lines to fix in the file {OUTPUT_CSV}.\nSee warnings above.")
    # TUMOR_TYPE check ?
    # check that SNOWMED IDENTIFIER is valid/active
    # curl -X 'GET' 'https://browser.ihtsdotools.org/snowstorm/snomed-ct/MAIN/SNOMEDCT-US/2024-03-01/concepts?offset=0&limit=100&termActive=true&ecl=707405009'


    # STEP 7: upload to ICA? ---- store metadata in another ICA Base table?

#################
if __name__ == '__main__':
    main()