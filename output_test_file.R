package_vec <- c("readr", "tidyr", "dplyr", "stringr", "forcats", "openxlsx")
lapply(package_vec, require, character.only = TRUE)

# raw data
raw_data_20 <- read_csv("jingfen_Dec_2020.csv", locale = locale(encoding = "GBK"))
raw_data_19 <- read_csv("jingfen_Dec_2019.csv", locale = locale(encoding = "GBK"))
third_dept_file <- read_csv("third_dept.csv", locale = locale(encoding = "GBK"))
raw_gmv_20 <- read.xlsx("2020_Dec_GMV.xlsx")
raw_gmv_19 <- read.xlsx("2019_Dec_GMV.xlsx")

column_names <- c("dt", "in_n_out", "search_tag", "brand_code", "brand_name", "first_dept", "second_dept", "third_dept", "prod_line", "impression", "click", "consumption", "ad_line", "ad_s_line", "ad_s_value", "ad_value")
colnames(raw_data_20) <- column_names
colnames(raw_data_19) <- column_names

column_names_gmv <- c("second_dept", "third_dept", "gmv_month")
colnames(raw_gmv_20) <- column_names_gmv
colnames(raw_gmv_19) <- column_names_gmv

raw_gmv_19$gmv_month <- as.numeric(raw_gmv_19$gmv_month)
raw_gmv_20$gmv_month <- as.numeric(raw_gmv_20$gmv_month)


prepare_raw_data <- function(df){
  df <- df %>% 
    filter(second_dept %in% c("POP业务部", "电脑配件业务部", "平板与台式机业务部", "音频业务部", "整机业务部", "智能网络与音频业务部", "文仪及配件业务部", "影像业务部")) %>% 
    mutate(second_dept = case_when(second_dept == "平板与台式机业务部" ~ "整机业务部", second_dept == "音频业务部" ~ "智能网络与音频业务部", TRUE ~ as.character(second_dept)))
}

prepare_gmv <- function(df){
  df <- df %>%
    filter(second_dept %in% c("POP业务部", "电脑配件业务部", "平板与台式机业务部", "音频业务部", "整机业务部", "智能网络与音频业务部", "文仪及配件业务部", "影像业务部")) %>%
    mutate(second_dept = case_when(second_dept == "平板与台式机业务部" ~ "整机业务部", second_dept == "音频业务部" ~ "智能网络与音频业务部", TRUE ~ as.character(second_dept)))
}

data_20 <- prepare_raw_data(raw_data_20)
data_19 <- prepare_raw_data(raw_data_19)
gmv_19 <- prepare_gmv(raw_gmv_19)
gmv_20 <- prepare_gmv(raw_gmv_20)

# ======================================================== second dept ==========================================================

# calc function

second_dept_data_calc <- function(df, gmv) {
  df1 <- df %>% 
    group_by(second_dept) %>% 
    summarize(across(impression:ad_value, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    mutate(ctr = click / impression, 
           cpc = consumption / click,
           cvr = ad_line / click,
           roi = ad_value / consumption,
           s = ad_s_value / ad_value)
  
  df2 <- df %>% 
    group_by(second_dept, search_tag) %>% 
    summarize(across(consumption, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    pivot_wider(id_cols = second_dept, names_from = search_tag, values_from = consumption) %>% 
    mutate(not_search = `非搜索` / (`非搜索` + `搜索`)) %>% 
    select(not_search)
  
  df3 <- df %>% 
    group_by(second_dept, in_n_out) %>% 
    summarize(across(consumption, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    pivot_wider(id_cols = second_dept, names_from = in_n_out, values_from = consumption) %>% 
    mutate(out = `站外` / (`站外` + `站内`)) %>% 
    select(out)
  
  df_all <- cbind(df1, df2, df3) %>% 
    mutate(consumption = consumption / 10000000) %>% 
    select(second_dept, consumption, ctr, cpc, cvr, roi, s, not_search, out)
  
  gmv <- gmv %>% 
    group_by(second_dept) %>% 
    summarize(across(gmv_month, ~ sum(., na.rm = TRUE))) %>% 
    ungroup()
  
  df_merge <- merge(df_all, gmv, by = "second_dept") %>% 
    mutate(feilv = consumption* 10000000 / gmv_month) %>% 
    select(second_dept, consumption, feilv, ctr, cpc, cvr, roi, s, not_search, out)
  
  return(df_merge)
}

data_20_update_second <- second_dept_data_calc(data_20, gmv_20)
data_19_update_second <- second_dept_data_calc(data_19, gmv_19)

yoy_df_second <- (data_20_update_second[, 2:10] - data_19_update_second[, 2:10]) / data_19_update_second[, 2:10]
yoy_df_second_ad_health <- yoy_df_second %>% 
  mutate(ad_health = 0.35 * (consumption * 0.5 + feilv * 0.5) + 0.3 * (-cpc * 0.3 + ctr * 0.3 + not_search * 0.2 + out * 0.2) + 0.35 * (cvr * 0.3 + roi * 0.5 + s * 0.2))


column_name_yoy_second <- str_c(colnames(yoy_df_second), "_yoy")
colnames(yoy_df_second) <- column_name_yoy_second

data_all_second <- cbind(data_20_update_second, yoy_df_second)
colnames(data_all_second) <- c("second_dept", "consumption_v", "feilv_v", "ctr_v", "cpc_v", "cvr_v", "roi_v", "s_v", "not_search_v", "out_v", column_name_yoy_second)

data_second_dept <- data_all_second %>% 
  pivot_longer(cols = consumption_v:out_yoy, names_to = c(".value", "cate_set"), 
               names_pattern = "(consumption|feilv|ctr|cpc|cvr|roi|s|not_search|out)_(.+)", values_drop_na = TRUE) %>% 
  mutate(cate = fct_recode(cate_set, `绝对值` = "v", `同比` = "yoy")) %>% 
  select(-c(cate_set, cate))

# first dept ============================================

first_dept_data_calc <- function(df, gmv) {
  df1 <- df %>% 
    group_by(first_dept) %>% 
    summarize(across(impression:ad_value, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    mutate(ctr = click / impression, 
           cpc = consumption / click,
           cvr = ad_line / click,
           roi = ad_value / consumption,
           s = ad_s_value / ad_value)
  
  df2 <- df %>% 
    group_by(first_dept, search_tag) %>% 
    summarize(across(consumption, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    pivot_wider(id_cols = first_dept, names_from = search_tag, values_from = consumption) %>% 
    mutate(not_search = `非搜索` / (`非搜索` + `搜索`)) %>% 
    select(not_search)
  
  df3 <- df %>% 
    group_by(first_dept, in_n_out) %>% 
    summarize(across(consumption, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    pivot_wider(id_cols = first_dept, names_from = in_n_out, values_from = consumption) %>% 
    mutate(out = `站外` / (`站外` + `站内`)) %>% 
    select(out)
  
  df_all <- cbind(df1, df2, df3) %>% 
    mutate(consumption = consumption / 10000000) %>% 
    select(first_dept, consumption, ctr, cpc, cvr, roi, s, not_search, out)
  
  gmv <- gmv %>% 
    summarize(across(gmv_month, ~ sum(., na.rm = TRUE)))
  
  df_merge <- cbind(df_all, gmv) %>% 
    mutate(feilv = consumption* 10000000 / gmv_month) %>% 
    select(consumption, feilv, ctr, cpc, cvr, roi, s, not_search, out)
  
  return(df_merge)
  
}


data_20_dt <- first_dept_data_calc(raw_data_20, raw_gmv_20)
data_19_dt <- first_dept_data_calc(raw_data_19, raw_gmv_19)

data_first_dept_yoy <- (data_20_dt - data_19_dt) / data_19_dt

data_first_dept <- rbind(data_20_dt, data_first_dept_yoy) %>%
  mutate(second_dept = "电脑数码事业部") %>% 
  select(second_dept, everything())


yoy_df_first_ad_health <- data_first_dept_yoy %>% 
  mutate(ad_health = 0.35 * (consumption * 0.5 + feilv * 0.5) + 0.3 * (-cpc * 0.3 + ctr * 0.3 + not_search * 0.2 + out * 0.2) + 0.35 * (cvr * 0.3 + roi * 0.5 + s * 0.2))

yoy_ad_health <- rbind(yoy_df_first_ad_health, yoy_df_second_ad_health)

yoy_ad_health_yoy <- yoy_ad_health %>% 
  mutate(ad_health_yoy = -10000, 
         ad_health = ad_health * 100) %>% 
  select(ad_health, ad_health_yoy) %>% 
  rename(ad_health_v = ad_health) %>% pivot_longer(cols = ad_health_v:ad_health_yoy, names_to = c(".value", "cate_set"), 
                                                   names_pattern = "(ad_health)_(.+)", values_drop_na = TRUE) %>% 
  mutate(cate = fct_recode(cate_set, `绝对值` = "v", `同比` = "yoy")) %>% 
  select(-c(cate_set, cate))

data_all_first_second <- rbind(data_first_dept, data_second_dept) 
data_all_first_second_ad_health <- cbind(data_all_first_second, yoy_ad_health_yoy) %>% 
  # select(!cvr) %>% 
  select(second_dept, ad_health, consumption, feilv, ctr, cpc, not_search, out, cvr, roi, s) %>% 
  rename(`二级部门` = second_dept, `广告收入（千万）` = consumption, `广告健康度` = ad_health, `费率` = feilv, `点击率` = ctr, 
         `点击成本` = cpc, `ROI` = roi, `收订率` = s,  
         `推荐占比` = not_search, `站外占比` = out, `转化率` = cvr)

# ===================================================== third dept ==============================================================

# calc function

third_dept_data_calc <- function(df, gmv) {
  df <- df %>% 
    mutate(third_dept = case_when(third_dept == "专业相机业务部" ~ "相机业务部", third_dept == "创新相机与游戏机业务部" ~ "相机业务部", TRUE ~ as.character(third_dept))) %>% 
    filter(third_dept %in% third_dept_file$V1)
  
  gmv <- gmv %>% 
    mutate(third_dept = case_when(third_dept == "传统相机业务部" ~ "相机业务部", third_dept == "新兴相机业务部" ~ "相机业务部", TRUE ~ as.character(third_dept))) %>% 
    filter(third_dept %in% third_dept_file$V1)
  
  df1 <- df %>% 
    group_by(second_dept, third_dept) %>% 
    summarize(across(impression:ad_value, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    mutate(ctr = click / impression, 
           cpc = consumption / click,
           cvr = ad_line / click,
           roi = ad_value / consumption,
           s = ad_s_value / ad_value)
  
  df2 <- df %>% 
    group_by(second_dept, third_dept, search_tag) %>% 
    summarize(across(consumption, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    pivot_wider(id_cols = c(second_dept, third_dept), names_from = search_tag, values_from = consumption) %>% 
    mutate(not_search = `非搜索` / (`非搜索` + `搜索`)) %>% 
    select(not_search)
  
  df3 <- df %>% 
    group_by(second_dept, third_dept, in_n_out) %>% 
    summarize(across(consumption, ~ sum(., na.rm = TRUE))) %>% 
    ungroup() %>% 
    pivot_wider(id_cols = c(second_dept, third_dept), names_from = in_n_out, values_from = consumption) %>% 
    mutate(out = `站外` / (`站外` + `站内`)) %>% 
    select(out)
  
  df_all <- cbind(df1, df2, df3) %>% 
    mutate(consumption = consumption / 10000000) %>% 
    select(second_dept, third_dept, consumption, ctr, cpc, cvr, roi, s, not_search, out)
  
  gmv <- gmv %>% 
    group_by(second_dept, third_dept) %>% 
    summarize(across(gmv_month, ~ sum(., na.rm = TRUE))) %>% 
    ungroup()  
  
  df_merge <- merge(df_all, gmv, by = c("second_dept", "third_dept")) %>% 
    mutate(feilv = consumption * 10000000 / gmv_month) %>% 
    select(second_dept, third_dept, consumption, feilv, ctr, cpc, cvr, roi, s, not_search, out)
  
  return(df_merge)
}

data_20_update <- third_dept_data_calc(data_20, gmv_20)
data_19_update <- third_dept_data_calc(data_19, gmv_19)

yoy_df <- (data_20_update[, 3:11] - data_19_update[, 3:11]) / data_19_update[, 3:11]

yoy_df_third_ad_health_yoy <- yoy_df %>% 
  mutate(ad_health = 0.35 * (consumption * 0.5 + feilv * 0.5) + 0.3 * (-cpc * 0.3 + ctr * 0.3 + not_search * 0.2 + out * 0.2) + 0.35 * (cvr * 0.3 + roi * 0.5 + s * 0.2)) %>% 
  mutate(ad_health_yoy = -10000, 
         ad_health = ad_health * 100) %>% 
  select(ad_health, ad_health_yoy) %>% 
  rename(ad_health_v = ad_health) %>% pivot_longer(cols = ad_health_v:ad_health_yoy, names_to = c(".value", "cate_set"), 
                                                   names_pattern = "(ad_health)_(.+)", values_drop_na = TRUE) %>% 
  mutate(cate = fct_recode(cate_set, `绝对值` = "v", `同比` = "yoy")) %>% 
  select(-c(cate_set, cate))

column_name_yoy <- str_c(colnames(yoy_df), "_yoy")
colnames(yoy_df) <- column_name_yoy

data_all <- cbind(data_20_update, yoy_df)
colnames(data_all) <- c("second_dept", "third_dept", "consumption_v", "feilv_v", "ctr_v", "cpc_v", 'cvr_v', "roi_v", "s_v", "not_search_v", "out_v", column_name_yoy)

data_third_dept <- data_all %>% 
  pivot_longer(cols = consumption_v:out_yoy, names_to = c(".value", "cate_set"), 
               names_pattern = "(consumption|feilv|ctr|cpc|cvr|roi|s|not_search|out)_(.+)", values_drop_na = TRUE) %>% 
  mutate(cate = fct_recode(cate_set, `绝对值` = "v", `同比` = "yoy")) %>% 
  select(-c(cate_set, cate))

data_third_dept_ad_health <- cbind(data_third_dept, yoy_df_third_ad_health_yoy) %>% 
  select(second_dept, third_dept, ad_health, consumption, feilv, ctr, cpc, not_search, out, cvr, roi, s) %>% 
  rename(`二级部门` = second_dept, `三级部门` = third_dept, `广告收入（千万）` = consumption, `费率` = feilv, `点击率` = ctr, 
         `点击成本` = cpc, `ROI` = roi, `收订率` = s,  
         `推荐占比` = not_search, `站外占比` = out, `转化率` = cvr, `广告健康度` = ad_health)


first_second_third = list(data_first_second_ad_health = data_all_first_second_ad_health, data_third_dept_ad_health = data_third_dept_ad_health)
write.xlsx(first_second_third,file = 'jingfen_Dec_dept_test6.xlsx', colWidths = "auto")
