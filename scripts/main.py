import pandas as pd
from sf_funcs import soql_to_pd_df




def main():
    # def extract_table_name_from_path(path_string):
    #     file_name_and_type = path_string.split("_")[-1]
    #     return file_name_and_type.split(".")[0]
        
    
    ## USE THIS IF YOU SEP OUT THE RESULTS MANUALLY
    # ALL_FILES = glob.glob("./data/*.xlsx")
    # ALL_TABLE_NAMES = [extract_table_name_from_path(path) for path in ALL_FILES]
    
    results_df = pd.read_excel("./data/Results.xlsx")
    
    ALL_TABLE_NAMES = results_df["TableName"].unique().tolist()
    
    for table_name in ALL_TABLE_NAMES: # ACHTUNG
        data_processing(results_df,table_name)



def data_processing(results_df, table_name_string):
    
    ########################
    ###       SETUP      ### 
    ########################
    
    TABLE_NAME = table_name_string
    OUTPUT_PATH =  "./output/MigrationMappingFor"
    OUTPUT_URL = OUTPUT_PATH + TABLE_NAME + ".xlsx"
    THRESHOLD = 0.8 # CHANGE THIS NUMBER IF YOU NEED A DIFFERENT THRESHOLD
    # FILTER_BY_PREFIX_LST = ["fferpcore__", "TM_GovSuite__", "peopleai__", "ZoomInfo_"]

    
    field_level_analysis_df_source = results_df[results_df["TableName"] == TABLE_NAME].copy()
    target_soql_custom_contract_df_source = soql_to_pd_df("SELECT Id, CreatedBy.Name, CreatedDate, DeveloperName, NamespacePrefix FROM CustomField WHERE EntityDefinition.QualifiedApiName = 'Account'", table_name_string, legacy_or_target_str="target")
    legacy_soql_custom_contract_df_source = soql_to_pd_df("SELECT Id, CreatedBy.Name, CreatedDate, DeveloperName, NamespacePrefix FROM CustomField WHERE EntityDefinition.QualifiedApiName = 'Account'", table_name_string, legacy_or_target_str="legacy")

    target_soql_data_types_source = soql_to_pd_df("SELECT MasterLabel, QualifiedApiName, DataType, Length, EntityDefinition.QualifiedApiName, NamespacePrefix FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName in ('Account')", table_name_string, legacy_or_target_str="target")
    legacy_soql_data_types_source = soql_to_pd_df("SELECT MasterLabel, QualifiedApiName, DataType, Length, EntityDefinition.QualifiedApiName, NamespacePrefix FROM FieldDefinition WHERE EntityDefinition.QualifiedApiName in ('Account')", table_name_string, legacy_or_target_str="legacy")
    


    field_level_analysis_df_copy = field_level_analysis_df_source[["ColumnName", "Type","Unq/NN/NN%"]].copy()
    target_soql_custom_contacts_df_copy = target_soql_custom_contract_df_source[["CreatedBy.Name","CreatedDate","DeveloperName","NamespacePrefix"]].copy().fillna("")
    legacy_soql_custom_contacts_df_copy = legacy_soql_custom_contract_df_source[["CreatedBy.Name","CreatedDate","DeveloperName","NamespacePrefix"]].copy().fillna("")
    target_soql_data_types_df_copy =  target_soql_data_types_source.copy()
    legacy_soql_data_types_df_copy =  legacy_soql_data_types_source.copy()

    #####################################################################
    # ANALYSIS 1: Migration Mapping Custom & Standard Objects FOR SHEET #
    #####################################################################
    
    
    ########################
    ###     PART 1       ### 
    ########################
    
    # GOAL: HANDLES ALL STANDARD OBJECTS
    def field_analysis_processor(field_level_analysis_df,legacy_soql_data_types_df,target_soql_data_types_df):
        def is_standard_object(name):
            return "Migrate" if '__c' not in name else ""

        def is_empty_standard_table(row):
            if row["Unq/NN/NN%"] == "0 / 0 / 0%" and '__c' not in row["ColumnName"]:
                return "Don't Migrate"
            else:
                return is_standard_object(row["ColumnName"])

        field_level_analysis_df_copy = field_level_analysis_df.copy()
        legacy_soql_data_types_df_copy = legacy_soql_data_types_df.copy()
        target_soql_data_types_df_copy = target_soql_data_types_df.copy()
        
        
        field_level_analysis_df_copy['StandardObject'] = field_level_analysis_df_copy.apply(is_empty_standard_table, axis=1)
        field_level_analysis_df_legacy = pd.merge(field_level_analysis_df_copy,legacy_soql_data_types_df_copy, how="left",left_on="ColumnName", right_on="QualifiedApiName").fillna("").rename(columns={"DataType":"DataType(Legacy)"})[["ColumnName", "Unq/NN/NN%", "Type", "StandardObject", "DataType(Legacy)"]]
        field_level_analysis_df_final = pd.merge(field_level_analysis_df_legacy,target_soql_data_types_df_copy, how="left",left_on="ColumnName", right_on="QualifiedApiName").fillna("").rename(columns={"DataType":"DataType(newOrg)"})[["ColumnName", "Unq/NN/NN%", "Type",  "DataType(Legacy)", "DataType(newOrg)", "StandardObject"]]
        return field_level_analysis_df_final


    field_level_analysis_df = field_analysis_processor(field_level_analysis_df_copy,legacy_soql_data_types_df_copy,target_soql_data_types_df_copy)
    ########################
    ###     PART 2       ### 
    ########################

    # GOAL: HANDLES ALL CUSTOM OBJECTS & checks if they exist
    
    def custom_contract_processor(soql_custom_contacts_df, soql_data_types_df, legacy_or_target_str="target"):
        def format_API_label(row):
            if row["NamespacePrefix"]:
                return row["NamespacePrefix"] + "__" + row["DeveloperName"] + "__c"
            else:
                return row["DeveloperName"] + "__c"
        legecy_or_newOrg_str = "newOrg" if legacy_or_target_str.lower().strip() == "target" else "legacy"
        soql_custom_contacts_df_copy= soql_custom_contacts_df.copy()
        soql_data_types_df_copy= soql_data_types_df.copy()
        result_df = ''

        
        if (not soql_custom_contacts_df_copy.empty) and (not soql_data_types_df_copy.empty):
            # print("here 1")
            soql_custom_contacts_df_copy["NamespacePrefix"] = soql_custom_contacts_df_copy["NamespacePrefix"].astype(str) #coerses NamespacePrefix to str type
            soql_custom_contacts_df_copy["ColumnName"] = soql_custom_contacts_df_copy.apply(format_API_label, axis=1) #applies func row-wise and returns correctly formatted str
            result_df = pd.merge(soql_custom_contacts_df_copy,soql_data_types_df_copy, how="left",left_on="ColumnName", right_on="QualifiedApiName").fillna("").rename(columns={"DataType":f"DataType({legecy_or_newOrg_str})"})[["ColumnName", f"DataType({legecy_or_newOrg_str})", "CreatedBy.Name", "CreatedDate"]]
        
        elif soql_custom_contacts_df_copy.empty:
            # print("here 2")
            result_df = soql_data_types_df_copy.fillna("").rename(columns={"QualifiedApiName": "ColumnName","DataType":f"DataType({legacy_or_target_str})"})[["ColumnName", f"DataType({legecy_or_newOrg_str})"]]
            result_df["CreatedDate"] = ""
        
        elif soql_data_types_df_copy.empty:
            # print("here 3")
            soql_custom_contacts_df_copy["NamespacePrefix"] = soql_custom_contacts_df_copy["NamespacePrefix"].astype(str) #coerses NamespacePrefix to str type
            soql_custom_contacts_df_copy["ColumnName"] = soql_custom_contacts_df_copy.apply(format_API_label, axis=1) #applies func row-wise and returns correctly formatted str
            result_df = soql_custom_contacts_df_copy #.fillna("")[["ColumnName", "DataType(newOrg)", "CreatedBy.Name", "CreatedDate"]]
        
        return result_df


    target_custom_contacts_df = custom_contract_processor(target_soql_custom_contacts_df_copy, target_soql_data_types_df_copy, "target")
    legacy_custom_contacts_df = custom_contract_processor(legacy_soql_custom_contacts_df_copy, legacy_soql_data_types_df_copy, "legacy")



    ########################
    ###      PART 3      ### 
    ########################

    # GOAL: Makes new df mapping all standard columns in legacy org and ONLY custom columns in new Org with the same name as in legacy org

    def is_standard_object_in_legacy_and_new_org(row):
        return "Migrate" if row["CreatedDate(newOrg)"] != ""  and row["Unq/NN/NN%"] != "0 / 0 / 0%" else ""


    def to_migrate(row):
        standard = row["StandardObject"]
        custom = row["CustomObject"]
        
        if standard and custom:
            return "Yes"
        elif standard:
            return "Yes"
        elif custom:
            return "Yes"
        else:
            return "No"
        
    def combine_datatypes_to_one_row(row):
        standard = row["DataType(newOrg)_StandardObject"]
        custom = row["DataType(newOrg)_CustomObject"]
        
        if standard == custom:
            return standard
        elif standard:
            return standard
        elif custom:
            return custom
        else:
            return ""
        
    required_col_names = ["ColumnName", "SqlDataType","Unq/NN/NN%", "CreatedDate(newOrg)", "CreatedBy.Name(newOrg)","CreatedDate(legacy)", "CreatedBy.Name(legacy)","DataType(Legacy)","DataType(newOrg)", "DataType(newOrg)_CustomObject","DataType(newOrg)_StandardObject","StandardObject", "CustomObject"]
    final_req_col_names = ["ColumnName", "SqlDataType","Unq/NN/NN%", "CreatedDate(newOrg)", "CreatedBy.Name(newOrg)","CreatedDate(legacy)", "CreatedBy.Name(legacy)","DataType(Legacy)","DataType(newOrg)","Migrate?"]
    analysis_1_df_merge_target = pd.merge(field_level_analysis_df,target_custom_contacts_df, how="left", on="ColumnName").fillna("").rename(columns={"Type": "SqlDataType"})
    analysis_1_df = pd.merge(analysis_1_df_merge_target,legacy_custom_contacts_df, how="left", on="ColumnName").fillna("").rename(columns={"DataType(newOrg)_y": "DataType(newOrg)_CustomObject",
                                                                                                                                           "DataType(newOrg)_x": "DataType(newOrg)_StandardObject",
                                                                                                                                           "CreatedBy.Name_x": "CreatedBy.Name(newOrg)", 
                                                                                                                                           "CreatedDate_x": "CreatedDate(newOrg)",
                                                                                                                                            "CreatedBy.Name_y": "CreatedBy.Name(legacy)", 
                                                                                                                                           "CreatedDate_y": "CreatedDate(legacy)"})
    
    
    analysis_1_df["CustomObject"] = analysis_1_df.apply(is_standard_object_in_legacy_and_new_org, axis=1)
    
    sheet1 = analysis_1_df[[col for col in required_col_names if col in analysis_1_df.columns and not analysis_1_df[col].isnull().all()]]
    
    # # sheet1_filtered = sheet1[~sheet1['ColumnName'].str.startswith(tuple(FILTER_BY_PREFIX_LST))] 
    
    sheet1_final = sheet1.copy()
    sheet1_final["Migrate?"] = sheet1_final.apply(to_migrate, axis=1)
    sheet1_final["DataType(newOrg)"] = sheet1_final.apply(combine_datatypes_to_one_row, axis=1)
    
    sheet1_final_filtered = sheet1_final[final_req_col_names]
    
    
    
    
    ####################################################################################################################################
    
    #HANDLE THIS LATER

    #############################################
    #         ANALYSIS 2: QA/QC FOR SHEET       #
    #############################################

    # ANALYSIS 3: Get the ones that weren't found and place them in a sheet so they can be manually inspected. SHOULD ONLY BE FOR CUSTOM FIELDS

    
    # def is_standard_column_name(name):
    #     return True if '__c' not in name else False

    # analysis_3_df = sheet1_final_filtered.copy() # changed
    # analysis_3_df["IsStandard"] = analysis_3_df["ColumnName"].map(is_standard_column_name)

    # filtered_analysis_3_df = analysis_3_df[analysis_3_df["IsStandard"]==False]
    # find_unique_names = lambda df1, df2: df1[~df1['ColumnName'].isin(df2['ColumnName'])]['ColumnName'].tolist()

    # unique_names_df1 = find_unique_names(filtered_analysis_3_df, soql_custom_contacts_df)
    # unique_names_df2 = find_unique_names(soql_custom_contacts_df, filtered_analysis_3_df)
    # max_len = max(len(unique_names_df1), len(unique_names_df2)) # this is to assure that these two variables can fit within the same DataFrame although they're two diff shapes
    
    # sheet2 = pd.DataFrame({
    #     'Field_Analysis_ColumnNames_Legacy': unique_names_df1 + [''] * (max_len - len(unique_names_df1)),
    #     'Soql_Custom_Accounts_ColumnNames_NewOrg': unique_names_df2 + [''] * (max_len - len(unique_names_df2))
    # })
    
    
    
    ###################################################################
    #      ANALYSIS 4: QA/QC Unmatched Similarity Analysis FOR TXT    #
    ###################################################################
    
    # GOAL : For Non-matching col names, provide txt file with probable similarities
    #        ^ This makes QA/QC much easier. Note the threshold number <- this is essential to keep in mind.



    # def levenshtein_similarity(str1, str2):
    #     similarity_score = fuzz.partial_ratio(str1, str2) / 100.0  # Output is between 0 and 1
    #     return similarity_score

    # def create_similarity_dict(column1, column2, threshold=THRESHOLD):
    #     similarity_dict = {}
        
    #     for word1 in column1:
    #         similarity_dict[word1] = []
    #         for word2 in column2:
    #             similarity = levenshtein_similarity(word1.lower(), word2.lower())
    #             if similarity >= threshold:
    #                 similarity_dict[word1].append((word2, round(similarity, 3)))
        
    #     return similarity_dict

    # def write_similarity_results_to_file(similarity_dict, output_path, filetype):
        
    #     if bool(similarity_dict): # empty dictionaries eval to False
    #         with open(output_path + TABLE_NAME + filetype, 'w') as f:
    #             for word1, similarities in similarity_dict.items():
    #                 f.write(f"\n{word1} (legacy):\n")
    #                 for word2, similarity in similarities:
    #                     f.write(f"\t{word2}: {similarity} (newOrg)\n\n")
    #     else:
    #         with open(output_path + TABLE_NAME + filetype, 'w') as f:
    #             f.write(f"NO WORD COMPARISONS MET THE THRESHOLD")


    # field_analysis_qa_qc = sheet3["Field_Analysis_ColumnNames_Legacy"].tolist()
    # soql_qa_qc = sheet3["Soql_Custom_Accounts_ColumnNames_NewOrg"].tolist()


    # qa_qc_results_dict = create_similarity_dict(field_analysis_qa_qc,soql_qa_qc)
    # final_qa_qc_results_dict = {}
    # if qa_qc_results_dict:
    #     final_qa_qc_results_dict = {k: v for k, v in qa_qc_results_dict.items() if v} # this removes any key/val pairs where the value is empty. Conditional is truthy
        
    
    # # TURNS QA/QC FINDINGS INTO PANDAS DF
    # qa_qc_results_df = pd.DataFrame.from_dict(final_qa_qc_results_dict, orient='index').transpose()
    # qa_qc_results_stack = qa_qc_results_df.stack().reset_index()
    # qa_qc_results_stack.columns = ['Index', 'LegacyOrgColName', 'NewOrgColName']
    
    # sheet3 = qa_qc_results_stack.drop(columns=["Index"]).sort_values("LegacyOrgColName").reset_index()
    
    
    
    #############################################################
    #############################################################
    #############################################################

    ############################################
    #      WRITE OUTPUT TO .XLSX & .TXT        #
    ############################################

    with pd.ExcelWriter(OUTPUT_URL) as writer: # NAMING CONVENTION ASSUMPTION
        print("----------------------------------------------------------")
        print(f"Saving {TABLE_NAME.upper()} to XLSX...")
        sheet1_final_filtered.to_excel(writer, sheet_name='FieldMapperAnalysis', index=False)
        # sheet2.to_excel(writer, sheet_name='SQLQueryBasedOnSheet1', index=False)
        # sheet2.to_excel(writer, sheet_name='Manual_QA_QC_check', index=False)
        # sheet3.to_excel(writer, sheet_name='Automatic_QA_QC_helper', index=False)
        # field_level_analysis_df_source.to_excel(writer, sheet_name='Original_Field_Analysis', index=False)
        # soql_custom_contacts_df_source.to_excel(writer, sheet_name='Original_Soql_Query', index=False)
        print("Data saved to .xlsx file!")
        




if __name__ == '__main__':
    main()