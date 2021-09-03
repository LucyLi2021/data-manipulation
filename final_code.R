library(data.table)
library(pryr)

# data_2021 <- fread("data_2021_01.csv")
data_2020_11 <- fread("data_2020_09.csv")
data_2020_12 <- fread("data_2020_10.csv")
data_2019_11 <- fread("data_2020_01_2.csv")
data_2019_12 <- fread("data_2019_01_2.csv")

data_all <- rbind(data_2020_11, data_2020_12, data_2019_11, data_2019_12)

colnames(data_all) <- c("dt", "union_type", "brand_code", 
                        "brand_name", "first_dept", "second_dept", "third_dept", 
                        "prod_line", "impression", "click", "consumption", 
                        "ad_line", "ad_s_line", "ad_s_value", "ad_value")

data_all_filter <- 
  data_all[second_dept %chin% c(
    "电脑配件业务部",
    "智能网络与音频业务部",
    "POP业务部",
    "整机业务部",
    "影像业务部",
    "整机与平板业务部",
    "文仪及配件业务部",
    "音频业务部",
    "平板与台式机业务部"
  ), ][, second_dept_update := 
         fcase(
           second_dept %chin% c("整机与平板业务部", "平板与台式机业务部"),
           "整机业务部",
           second_dept == "音频业务部",
           "智能网络与音频业务部",
           second_dept == "电脑配件业务部",
           "电脑配件业务部",
           second_dept == "智能网络与音频业务部",
           "智能网络与音频业务部",
           second_dept == "POP业务部",
           "POP业务部",
           second_dept == "整机业务部",
           "整机业务部",
           second_dept == "影像业务部",
           "影像业务部",
           second_dept == "文仪及配件业务部",
           "文仪及配件业务部"
         )]

unneeded_third_dept <- data_all[, unique(third_dept)][grep(".+\\(历史\\).*", data_all[, unique(third_dept)])]
unneeded_third_dept <- c(unneeded_third_dept, "未知")

data_all_filter <- data_all_filter[!third_dept %chin% unneeded_third_dept, ]

data_dept_brand <-
  data_all_filter[, lapply(.SD, sum, na.rm = TRUE), by = c("dt", "union_type", "second_dept_update", "third_dept", "brand_name", "prod_line"),
                  .SDcols = c(
                    "impression",
                    "click",
                    "consumption",
                    "ad_line",
                    "ad_s_line",
                    "ad_s_value",
                    "ad_value"
                  )]

data_dept_brand_2 <-
  data_dept_brand[, 
                  lapply(.SD, sum, na.rm = TRUE), keyby = c("dt", "union_type", "second_dept_update", "third_dept", "brand_name", "prod_line"),
                  .SDcols = c(
                    "impression",
                    "click",
                    "consumption",
                    "ad_line",
                    "ad_s_line",
                    "ad_s_value",
                    "ad_value"
                  )]

data_dept_brand_2[, c("year_calc", "month_date_calc", "cvr", "ctr", "roi", "cpc") := .(year(dt), 
                                                                                       substr(as.character(dt), start = 6, stop = 10), 
                                                                                       ad_line / click, 
                                                                                       click / impression, 
                                                                                       ad_value / consumption, 
                                                                                       consumption / click)]

new_order <- c(colnames(data_dept_brand_2)[colnames(data_dept_brand_2) != "union_type"], 
               colnames(data_dept_brand_2)[colnames(data_dept_brand_2) == "union_type"])

setcolorder(data_dept_brand_2, neworder = new_order)

output_data_2 <- function(df, output_file_path) {
  
  exist_file_num <- length(list.files(output_file_path))
  
  df_size <- as.integer(object_size(df))
  if (df_size > 4.5E7) {
    file_num <- df_size %/% 4.5E7 + 1
    row_num <- nrow(df) %/% file_num
    for (file_index in 1:(file_num-1)) {
      output_df <- df[((file_index-1)*row_num+1):(file_index*row_num), ]
      write.table(output_df, file = paste0("test_output_file/data_source_", file_index+exist_file_num, ".txt"), fileEncoding = 'UTF-8', row.names = FALSE, col.names = FALSE, sep = ',')
    }
    final_df <- df[((file_num-1)*row_num+1):nrow(df), ]
    write.table(final_df, file = paste0("test_output_file/data_source_", file_num+exist_file_num, ".txt"), fileEncoding = 'UTF-8', row.names = FALSE, col.names = FALSE, sep = ',')
  } else {
    write.table(df, file = paste0("test_output_file/data_source_", 1+exist_file_num, ".txt"), fileEncoding = 'UTF-8', row.names = FALSE, col.names = FALSE, sep = ',')
  }
  
}

output_data_2(data_dept_brand_2, 'test_output_file')