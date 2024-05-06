# connected_insights_metadata_generation

Create metadata CSV table from data in Clarity

## Command-line usage

![Image](https://github.com/keng404/connected_insights_metadata_generation/blob/main/script_usage_help.png)

### Detailed parameter usage

- ```--api_key {STR}``` or ```--api_key_file {FILE}``` to specify API KEY
- ```--sample_id sample1 sample2 sample3``` or ```--lims_sample_project {STR}``` to specify what set of samples to generate metadata CSV for
- ```--project_id {ALPHANUMERIC_STR}``` or ```--project_name {STR}``` to specify ICA project
- ```--lenient_mode``` is a flag that will generate a CSV that can be manually modified before ingestion to Connnected Insights.
If this flag is not included on command line, script will error out if there are lines that don't have all mandatory fields or optional fields (if these are specified)

### Additional Notes

- Base Table is hard-coded to ```CLARITY_SAMPLE_VIEW_tenant```
    - The 'Data' field is parsed to identify mandatory fields ```Sample_ID,Tumor_Type,Case_ID``` needed for case ingestion by Connected Insights.
    This includes userDefinedFields. 
- For now there is no validation of ```Tumor_Type``` as a valid SNOWMEDCT ID. This can be added in
- Currently no integration with Connected Insights API to ingest this case metadata or to grab all mandatory(i.e. required) fields tied to a Test_Definition or Workflow_ID that has been configured in users Connected Insights workgroup.

# connected_insights_case_metadata_upload

Upload metadata CSV table into Connected Insights

## Command-line usage

![Image](https://github.com/keng404/connected_insights_metadata_generation/blob/main/Help_screenshot.connected_insights_case_metadata_upload.png)

## Installation of python modules to run script

``` bash
pip3 install -r requirements.txt
```

This script has been developed and tested in Python 3.9.6.

For convenience, you can also use the docker image ```keng404/connected_insights_metadata_generation:0.0.1``` for testing and development. 

You can view the docker repository [here](https://hub.docker.com/repository/docker/keng404/connected_insights_metadata_generation/general)
