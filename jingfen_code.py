# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 09:44:50 2021

@author: liwanning6
"""

# Import all the packages we need
import os
import numpy as np
import pandas as pd

# get current working directory and change it into the directory where the jingfen raw data stored
os.getcwd()
os.chdir("D://liwanning6/Downloads/jingfen_data/")

# Load the raw data for this month and last month
raw_data_20 = pd.read_csv("jingfen_Dec_2020.csv", encoding = "GBK")
raw_data_19 = pd.read_csv("jingfen_Dec_2019.csv", encoding = "GBK")

# Load the raw gmv data for this month and last month
third_dept_file = pd.read_csv("third_dept.csv", encoding = "GBK")
raw_gmv_20 = pd.read_excel("2020_Dec_GMV.xlsx", encoding = "GBK")
raw_gmv_19 = pd.read_excel("2019_Dec_GMV.xlsx", encoding = "GBK")

# change the original cloumn names into English cloumn names for later convenient purpose
column_names = ["dt", "in_n_out", "search_tag", "brand_code", "brand_name", "first_dept", "second_dept", "third_dept", "prod_line", "impression", "click", "consumption", "ad_line", "ad_s_line", "ad_s_value", "ad_value"]
raw_data_20.columns = column_names
raw_data_19.columns = column_names

# change the raw gmv data column names
column_names_gmv = ["second_dept", "third_dept", "gmv_month"]
raw_gmv_20.columns = column_names_gmv
raw_gmv_19.columns = column_names_gmv

# transform the month data into integer type to avoid bugs
raw_gmv_19["gmv_month"] = raw_gmv_19["gmv_month"].astype('int64')
raw_gmv_20["gmv_month"] = raw_gmv_20["gmv_month"].astype('int64')

# build prepare raw data function and filter and correct the second department names
def prepare_raw_data(df):
    
    # build the filter to filter out unneeded second department
    second_dept_filter = df.second_dept.isin(["电脑配件业务部", "智能网络与音频业务部", "POP业务部", "整机业务部", "影像业务部", 
                          "整机与平板业务部", "文仪及配件业务部", "音频业务部", "平板与台式机业务部"])

    df = df.loc[second_dept_filter, :]
    
    # correct the second department name
    df.loc[:, "second_dept"] = np.where(df.second_dept.isin(["整机与平板业务部", "平板与台式机业务部"]), "整机业务部", 
         np.where(df.second_dept.isin(["音频业务部"]), "智能网络与音频业务部", df.loc[:, "second_dept"]))
    
    return df

# using prepare raw data function to clean the raw data
data_20 = prepare_raw_data(raw_data_20)
data_19 = prepare_raw_data(raw_data_19)
gmv_20 = prepare_raw_data(raw_gmv_20)
gmv_19 = prepare_raw_data(raw_gmv_19)

# build the second department calculate function to directly output the calculate result of second department
def second_dept_data_calc(df, gmv):
    
    # drop the brand code column and group by second department and do the sum calculation
    df1 = df.drop("brand_code", axis=1).groupby(['second_dept']).sum().reset_index()
    
    #calculate cvr, ctr, roi, cpc and shou ding lv
    df1["cvr"] = df1.loc[:, "ad_line"] / df1.loc[:, "click"]
    df1["ctr"] = df1.loc[:, "click"] / df1.loc[:, "impression"]
    df1["roi"] = df1.loc[:, "ad_value"] / df1.loc[:, "consumption"]
    df1["cpc"] = df1.loc[:, "consumption"] / df1.loc[:, "click"]
    df1["s"] = df1.loc[:, "ad_s_value"] / df1.loc[:, "ad_value"]
    
    # calcuate the not search ratio, not search ratio = recommendation ratio (you probably need to change this calculation later!)
    df2 = df.loc[:, ["second_dept", "search_tag", "consumption"]].groupby(["second_dept", "search_tag"]).sum().reset_index()
    df2 = df2.pivot(index="second_dept", columns="search_tag", values="consumption").reset_index()
    df2["not_search"] = df2.loc[:, "非搜索"] / (df2.loc[:, "搜索"] + df2.loc[:, "非搜索"])
    
    # calculate the zhan nei and zhanwai ratio
    df3 = df.loc[:, ["second_dept", "in_n_out", "consumption"]].groupby(["second_dept", "in_n_out"]).sum().reset_index()
    df3 = df3.pivot(index="second_dept", columns="in_n_out", values="consumption").reset_index()
    df3["out"] = df3.loc[:, "站外"] / (df3.loc[:, "站外"] + df3.loc[:, "站内"])
    
    # concatnate 3 dataframes togather
    df_all = pd.concat([df1, df2.loc[:, "not_search"], df3.loc[:, "out"]], axis=1)
    
    # group by gmv data and do the calculation
    gmv = gmv.loc[:, ["second_dept", "gmv_month"]].groupby("second_dept").sum().reset_index()
    
    
    df_merge = pd.merge(df_all, gmv, how = 'inner', on = 'second_dept')
    df_merge["feilv"] = df_merge.loc[:, "consumption"] / df_merge.loc[:, "gmv_month"]
    df_merge["consumption"] = df_merge.loc[:, "consumption"] / 10000000
    
    df_final = df_merge.loc[:, ["second_dept", "consumption", "feilv", "ctr", "cpc", "cvr", "roi", "s", "not_search", "out"]]
    return df_final

data_20_update_second = second_dept_data_calc(data_20, gmv_20)
data_19_update_second = second_dept_data_calc(data_19, gmv_19)

yoy_df_second = (data_20_update_second.drop("second_dept", axis=1) - data_19_update_second.drop("second_dept", axis=1)) / data_19_update_second.drop("second_dept", axis=1)

yoy_df_second_ad_health = yoy_df_second.copy(deep=True)
yoy_df_second_ad_health["ad_health"] = 0.35 * (yoy_df_second_ad_health["consumption"] * 0.5 + yoy_df_second_ad_health["feilv"] * 0.5) + 0.3 * (-yoy_df_second_ad_health["cpc"] * 0.3 + yoy_df_second_ad_health["ctr"] * 0.3 + yoy_df_second_ad_health["not_search"] * 0.2 +  yoy_df_second_ad_health["out"] * 0.2) + 0.35 * (yoy_df_second_ad_health["cvr"] * 0.3 + yoy_df_second_ad_health["roi"] * 0.5 + yoy_df_second_ad_health["s"] * 0.2)

yoy_df_second_ad_health.loc[:, "second_dept"] = data_20_update_second.second_dept
data_second_dept = pd.concat([data_20_update_second, yoy_df_second_ad_health]).sort_index()

def first_dept_data_calc(df, gmv):
    df1 = df.drop("brand_code", axis=1).groupby(['first_dept']).sum().reset_index()
    df1["cvr"] = df1.loc[:, "ad_line"] / df1.loc[:, "click"]
    df1["ctr"] = df1.loc[:, "click"] / df1.loc[:, "impression"]
    df1["roi"] = df1.loc[:, "ad_value"] / df1.loc[:, "consumption"]
    df1["cpc"] = df1.loc[:, "consumption"] / df1.loc[:, "click"]
    df1["s"] = df1.loc[:, "ad_s_value"] / df1.loc[:, "ad_value"]
    
    df2 = df.loc[:, ["first_dept", "search_tag", "consumption"]].groupby(["first_dept", "search_tag"]).sum().reset_index()
    df2 = df2.pivot(index="first_dept", columns="search_tag", values="consumption").reset_index()
    df2["not_search"] = df2.loc[:, "非搜索"] / (df2.loc[:, "搜索"] + df2.loc[:, "非搜索"])
    
    df3 = df.loc[:, ["first_dept", "in_n_out", "consumption"]].groupby(["first_dept", "in_n_out"]).sum().reset_index()
    df3 = df3.pivot(index="first_dept", columns="in_n_out", values="consumption").reset_index()
    df3["out"] = df3.loc[:, "站外"] / (df3.loc[:, "站外"] + df3.loc[:, "站内"])
    
    df_all = pd.concat([df1, df2.loc[:, "not_search"], df3.loc[:, "out"]], axis=1)
    
    gmv.loc[:, "first_dept"] = "电脑数码事业部"
    gmv = gmv.loc[:, ["first_dept", "gmv_month"]].groupby('first_dept').sum().reset_index().drop("first_dept", axis=1)
    
    df_merge = pd.concat([df_all, gmv], axis=1)
    df_merge["feilv"] = df_merge.loc[:, "consumption"] / df_merge.loc[:, "gmv_month"]
    df_merge["consumption"] = df_merge.loc[:, "consumption"] / 10000000
    
    df_final = df_merge.loc[:, ["consumption", "feilv", "ctr", "cpc", "cvr", "roi", "s", "not_search", "out"]]
    return df_final

data_20_dt = first_dept_data_calc(raw_data_20, raw_gmv_20)
data_19_dt = first_dept_data_calc(raw_data_19, raw_gmv_19)
yoy_df_first = (data_20_dt - data_19_dt) / data_19_dt

yoy_df_first_ad_health = yoy_df_first.copy(deep=True)
yoy_df_first_ad_health["ad_health"] = 0.35 * (yoy_df_first_ad_health["consumption"] * 0.5 + yoy_df_first_ad_health["feilv"] * 0.5) + 0.3 * (-yoy_df_first_ad_health["cpc"] * 0.3 + yoy_df_first_ad_health["ctr"] * 0.3 + yoy_df_first_ad_health["not_search"] * 0.2 +  yoy_df_first_ad_health["out"] * 0.2) + 0.35 * (yoy_df_first_ad_health["cvr"] * 0.3 + yoy_df_first_ad_health["roi"] * 0.5 + yoy_df_first_ad_health["s"] * 0.2)

data_first_dept = pd.concat([data_20_dt, yoy_df_first_ad_health], axis=0)
data_first_dept.loc[:, "second_dept"] = "电脑数码事业部"

data_second_dept.index = np.repeat(list(np.arange(0.5, 3.5, 0.5)), 2)

data_all_first_second = pd.concat([data_second_dept, data_first_dept], axis=0).sort_index().reset_index()
data_all_first_second = data_all_first_second.loc[:, ["second_dept", "ad_health", "consumption", "feilv", "ctr", "cpc", "not_search", "out", "cvr", "roi", "s"]]
data_all_first_second.columns = ["二级部门", "广告健康度", "广告收入（千万）", "费率", "点击率", "点击成本", "推荐占比", "站外占比", "转化率", "ROI", "收订率"]


def third_dept_data_calc(df, gmv):
    df = df.loc[df["third_dept"].isin(list(third_dept_file["V1"]))]
    df.loc[:, "third_dept"] = np.where(df.third_dept.isin(["专业相机业务部", "创新相机与游戏机业务部"]), "相机业务部", df.loc[:, "third_dept"])
    gmv = gmv.loc[gmv["third_dept"].isin(list(third_dept_file["V1"]))]
    gmv.loc[:, "third_dept"] = np.where(gmv.third_dept.isin(["传统相机业务部", "新兴相机业务部"]), "相机业务部", gmv.loc[:, "third_dept"])
    
    df1 = df.drop("brand_code", axis=1).groupby(['second_dept', 'third_dept']).sum().reset_index()
    df1["cvr"] = df1.loc[:, "ad_line"] / df1.loc[:, "click"]
    df1["ctr"] = df1.loc[:, "click"] / df1.loc[:, "impression"]
    df1["roi"] = df1.loc[:, "ad_value"] / df1.loc[:, "consumption"]
    df1["cpc"] = df1.loc[:, "consumption"] / df1.loc[:, "click"]
    df1["s"] = df1.loc[:, "ad_s_value"] / df1.loc[:, "ad_value"]
    
    df2 = df.loc[:, ["second_dept", "third_dept", "search_tag", "consumption"]].groupby(["second_dept", "third_dept", "search_tag"]).sum().reset_index()
    df2 = df2.pivot_table(index=["second_dept", "third_dept"], columns="search_tag", values="consumption").reset_index()
    df2["not_search"] = df2.loc[:, "非搜索"] / (df2.loc[:, "搜索"] + df2.loc[:, "非搜索"])
    
    df3 = df.loc[:, ["second_dept", "third_dept", "in_n_out", "consumption"]].groupby(["second_dept", "third_dept", "in_n_out"]).sum().reset_index()
    df3 = df3.pivot_table(index=["second_dept", "third_dept"], columns="in_n_out", values="consumption").reset_index()
    df3["out"] = df3.loc[:, "站外"] / (df3.loc[:, "站外"] + df3.loc[:, "站内"])
    
    df_all = pd.concat([df1, df2.loc[:, "not_search"], df3.loc[:, "out"]], axis=1)
    
    gmv = gmv.loc[:, ["second_dept", "third_dept", "gmv_month"]].groupby(["second_dept", "third_dept"]).sum().reset_index()
    
    df_merge = pd.merge(df_all, gmv, how = 'inner', on = ['second_dept', 'third_dept'])
    df_merge["feilv"] = df_merge.loc[:, "consumption"] / df_merge.loc[:, "gmv_month"]
    df_merge["consumption"] = df_merge.loc[:, "consumption"] / 10000000
    
    df_final = df_merge.loc[:, ["second_dept", "third_dept", "consumption", "feilv", "ctr", "cpc", "cvr", "roi", "s", "not_search", "out"]]
    return df_final

data_20_update = third_dept_data_calc(data_20, gmv_20)
data_19_update = third_dept_data_calc(data_19, gmv_19)

yoy_df = (data_20_update.drop(["second_dept", "third_dept"], axis=1) - data_19_update.drop(["second_dept", "third_dept"], axis=1)) / data_19_update.drop(["second_dept", "third_dept"], axis=1)

yoy_df_third_ad_health = yoy_df.copy(deep=True)
yoy_df_third_ad_health["ad_health"] = 0.35 * (yoy_df_third_ad_health["consumption"] * 0.5 + yoy_df_third_ad_health["feilv"] * 0.5) + 0.3 * (-yoy_df_third_ad_health["cpc"] * 0.3 + yoy_df_third_ad_health["ctr"] * 0.3 + yoy_df_third_ad_health["not_search"] * 0.2 +  yoy_df_third_ad_health["out"] * 0.2) + 0.35 * (yoy_df_third_ad_health["cvr"] * 0.3 + yoy_df_third_ad_health["roi"] * 0.5 + yoy_df_third_ad_health["s"] * 0.2)

yoy_df_third_ad_health[["second_dept", "third_dept"]] = data_20_update.loc[:, ["second_dept", "third_dept"]]
data_third_dept = pd.concat([data_20_update, yoy_df_third_ad_health]).sort_index()

data_all_third = data_third_dept.loc[:, ["second_dept", "third_dept", "ad_health", "consumption", "feilv", "ctr", "cpc", "not_search", "out", "cvr", "roi", "s"]]
data_all_third.columns = ["二级部门", "三级部门", "广告健康度", "广告收入（千万）", "费率", "点击率", "点击成本", "推荐占比", "站外占比", "转化率", "ROI", "收订率"]


with pd.ExcelWriter('jingfen_Dec_dept.xlsx') as writer:  
    data_all_first_second.to_excel(writer, sheet_name = '一级二级部门', index = False)
    data_all_third.to_excel(writer, sheet_name = '三级部门', index = False)