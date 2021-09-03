# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 19:03:06 2021

@author: liwanning6
"""

import os
import numpy as np
import pandas as pd
os.getcwd()

def load_data():
    
    def wd_path_creator():      
            
        wd_name = input("Please input working directory name: \n")
        
        try:
            os.chdir("D://liwanning6/Downloads/"+wd_name)
        except FileNotFoundError:
            print("File name does not exist under downloads.")

        print("These are the files staying in the working directory.\n")
        
        for file_name in os.listdir():
            print(file_name)
            
        check_answer = input("Please double check working directory (Y/N)")
        return check_answer
        
    check_answer = wd_path_creator()
    
    while check_answer == "N":
        os.chdir("D://liwanning6/Downloads/")
        check_answer = wd_path_creator()
        
    else:
        raw_data_file_name = input("Please enter the file name: \n")
        test_df = pd.read_csv(raw_data_file_name)
        print("Raw data file loaded successfully!")
        return test_df
        
        
test_df = load_data()

def prepare_raw_data(df):
    
    df.columns = ["dt", "union_type", "brand_code", 
                        "brand_name", "first_dept", "second_dept", "third_dept", 
                        "prod_line", "impression", "click", "consumption", 
                        "ad_line", "ad_s_line", "ad_s_value", "ad_value"]
    
    second_dept_filter = df.second_dept.isin(["电脑配件业务部", "智能网络与音频业务部", "POP业务部", "整机业务部", "影像业务部", 
                          "整机与平板业务部", "文仪及配件业务部", "音频业务部", "平板与台式机业务部"])

    df = df.loc[second_dept_filter, :]
    
    df.loc[:, "second_dept_update"] = np.where(df.second_dept.isin(["整机与平板业务部", "平板与台式机业务部"]), "整机业务部", 
         np.where(df.second_dept.isin(["音频业务部"]), "智能网络与音频业务部", df.loc[:, "second_dept"]))
    
    df_filter = df.loc[(~df.loc[:, "third_dept"].str.contains("历史")) & (df.loc[:, "third_dept"] != "未知"), ]
    
    df_filter.drop("brand_code", axis=1)
    
    df_sum = df_filter.groupby(["dt", "union_type", "second_dept_update", "third_dept", "brand_name", "prod_line"]).sum().reset_index()
    
    df_sum["year_calc"] = df_sum.loc[:, "dt"].str.slice(0, 4)
    df_sum["month_date_calc"] = df_sum.loc[:, "dt"].str.slice(5, 10)
    df_sum["cvr"] = df_sum.loc[:, "ad_line"] / df_sum.loc[:, "click"]
    df_sum["ctr"] = df_sum.loc[:, "click"] / df_sum.loc[:, "impression"]
    df_sum["roi"] = df_sum.loc[:, "ad_value"] / df_sum.loc[:, "consumption"]
    df_sum["cpc"] = df_sum.loc[:, "consumption"] / df_sum.loc[:, "click"]

    original_colorder = df_sum.columns.tolist()
    new_colorder = [original_colorder[0]] + original_colorder[2:] + [original_colorder[1]]
    df_sum = df_sum.loc[:, new_colorder]
    
    return df_sum

test_df_sum = prepare_raw_data(test_df)

test_df_sum.head()

# output function for outputting dataframe
def output_data(df, output_file_path):
    df_size = df.memory_usage().sum()
    exist_file_num = len(os.listdir(output_file_path))
    
    if df_size > 4.5E7 : 
        file_num = df_size // 4.5E7 + 1
        row_num = df.shape[0] // file_num
        for file_index in range(1, np.int(file_num)):
            output_df = df.loc[((file_index-1)*row_num):((file_index*row_num)-1), :]
            output_df.to_csv('test_output_file/data_source_' + str(np.int(file_index + exist_file_num)) + '.txt', sep = ',', index = False, header = False, encoding = 'utf-8') 
        final_df = df.loc[((file_num-1)*row_num):(df.shape[0]), :]
        final_df.to_csv('test_output_file/data_source_' + str(np.int(file_num + exist_file_num)) + '.txt', sep = ',', index = False, header = False, encoding = 'utf-8')
    else:
        df.to_csv('test_output_file/data_source_' + str(1 + np.int(exist_file_num)) + '.txt', sep = ',', index = False, header = False, encoding = 'utf-8')

output_data(test_df_sum, "test_output_file")