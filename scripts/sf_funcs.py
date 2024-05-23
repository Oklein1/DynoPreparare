import requests
import pandas as pd
import creds
import io

def soql_to_pd_df(soql_query_str, table_name_str, legacy_or_target_str="target"):
    
    ######################################
    ######################################
    ####       GLOBAL VARIABLES       ####
    ######################################
    ######################################
    
    leg_or_tar_str = legacy_or_target_str.lower().strip()
    
    if leg_or_tar_str == "legacy":
        INSTANT_URL = creds.inst_url_legacy
        ACCESS_TOKEN = creds.acc_token_legacy
        
    else:
        
        INSTANT_URL = creds.inst_url
        ACCESS_TOKEN = creds.acc_token


    ######################################
    ######################################
    ####          FUNCTIONS           ####
    ######################################
    ######################################

    def soql_URL_generator(soql_query_str, instant_url, table_name_str='Account'):
        def url_generator(soql_query_str, instant_url):
            url_path = "/services/data/v59.0/tooling/query/?q="
            soql_url = soql_query_str.replace(", ", ",").replace(" ", "+")
            return instant_url + url_path + soql_url
        
        if table_name_str == 'Account':
            return url_generator(soql_query_str, instant_url)
        else:
            updated_soql_query_str = soql_query_str.replace('Account', table_name_str.title())
            return url_generator(updated_soql_query_str, instant_url)
        

    def soql_column_name_extractor(soql_query_str):
        soql_split_on_SELECT_str = soql_query_str.split("SELECT")[1]
        soql_split_on_FROM_str = soql_split_on_SELECT_str.split("FROM")[0]
        soql_split_FINAL_str = soql_split_on_FROM_str.strip().split(",") #ensures elimination of whitespace and then splits the string sep by commas into list. Will loop over this later
        return  soql_split_FINAL_str #[col_name.split(".")[0] for col_name in soql_split_FINAL_str]


    def sf_json_to_pd_df(json_req_response, query_col_list):
        def sf_json_data_extractor(query_col_list):
            def query_col_to_dict(query_col_list):
                storage = {}
                for col_name in query_col_list:
                    storage[col_name.strip()] = [] #refactor
                return storage
            
            
            results_dict = query_col_to_dict(query_col_list)
            source_df = pd.read_json(io.StringIO(json_req_response.decode()))["records"]
            
            
            for json_data in source_df:
                for query_col_str in query_col_list:
                    query_col_STRIPED_str = query_col_str.strip()
                    if "." in query_col_STRIPED_str:
                        query_col_split_list = query_col_STRIPED_str.split(".")
                        parent_key = query_col_split_list[0]
                        child_key = query_col_split_list[1]
                        if parent_key in json_data and json_data[parent_key] is not None and child_key in json_data[parent_key]:
                            results_dict[query_col_STRIPED_str].append(json_data[parent_key][child_key])
                        else:
                            results_dict[query_col_STRIPED_str].append(None)
                    else:
                        if query_col_STRIPED_str in json_data and json_data[query_col_STRIPED_str] is not None:
                            results_dict[query_col_STRIPED_str].append(json_data[query_col_STRIPED_str])
                        else:
                            results_dict[query_col_STRIPED_str].append(None)
            return pd.DataFrame.from_dict(results_dict)
        
        return sf_json_data_extractor(query_col_list)




    

    ######################################
    ######################################
    ####          MAIN CODE           ####
    ######################################
    ######################################



    headers = {
    'Authorization': 'Bearer '+ ACCESS_TOKEN,
    'Content-Type': 'application/json'
    }

    # TEST 1
    # query_col_list = soql_column_name_extractor("SELECT Id, CreatedBy.Name, CreatedDate, DeveloperName, NamespacePrefix FROM CustomField WHERE EntityDefinition.QualifiedApiName = 'Account'")
    # job_creation_url = soql_URL_generator("SELECT Id, CreatedBy.Name, CreatedDate, DeveloperName, NamespacePrefix FROM CustomField WHERE EntityDefinition.QualifiedApiName = 'Account'", INSTANT_URL, "Opportunity")

    # TEST 2
    # job_creation_url = soql_URL_generator("SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, DataType FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName IN ('Account')", inst_url, "Account")
    # query_col_list = soql_column_name_extractor("SELECT EntityDefinition.QualifiedApiName, QualifiedApiName, DataType FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName IN ('Account')")


    query_col_list = soql_column_name_extractor(soql_query_str)
    job_creation_url = soql_URL_generator(soql_query_str, INSTANT_URL, table_name_str)
    
    job_resp = requests.request('GET',job_creation_url,headers=headers).content

    
    return sf_json_to_pd_df(job_resp, query_col_list)

