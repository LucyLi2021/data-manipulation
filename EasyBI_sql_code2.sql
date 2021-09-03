WITH
	a AS
	(
		SELECT
			second_dept_update,
			third_dept,
			brand_name,
			prod_line,
			SUM(impression) AS imp_dt_1,
			SUM(click) AS click_dt_1,
			SUM(consumption) AS consumption_dt_1,
			SUM(ad_line) AS ad_line_dt_1,
			SUM(ad_value) AS ad_value_dt_1 
		FROM
			wangrui_test_view
		WHERE dt >= #dt_1# AND dt <= #dt_1_end# AND #{filter|field=second_dept_update} AND #{filter|field=brand_name, fuzzy=yes} AND #{filter|field=prod_line} AND #{filter|field=third_dept}
		GROUP BY
			second_dept_update,
			third_dept,
			brand_name,
			prod_line
	)
	,
	a_1 AS 
	(
		SELECT
			second_dept_update,
			third_dept,
			brand_name,
			prod_line,
			SUM(impression) AS imp_dt_2,
			SUM(click) AS click_dt_2,
			SUM(consumption) AS consumption_dt_2,
			SUM(ad_line) AS ad_line_dt_2,
			SUM(ad_value) AS ad_value_dt_2
		FROM
			wangrui_test_view
		WHERE dt >= #dt_2# AND dt <= #dt_2_end# AND #{filter|field=second_dept_update} AND #{filter|field=brand_name, fuzzy=yes} AND #{filter|field=prod_line} AND #{filter|field=third_dept}
		GROUP BY
			second_dept_update,
			third_dept,
			brand_name,
			prod_line
	)
	,
	b AS
	(
		SELECT
			a.second_dept_update,
			a.third_dept,
			a.brand_name, 
			a.prod_line,
			a.imp_dt_1,
			a.click_dt_1,
			a.consumption_dt_1,
			a.ad_line_dt_1,
			a.ad_value_dt_1,
			a_1.imp_dt_2,
			a_1.click_dt_2,
			a_1.consumption_dt_2,
			a_1.ad_line_dt_2,
			a_1.ad_value_dt_2
		FROM
			a
		LEFT OUTER JOIN a_1
		ON a.second_dept_update = a_1.second_dept_update AND a.third_dept = a_1.third_dept AND a.brand_name = a_1.brand_name AND a.prod_line = a_1.prod_line
	)
SELECT 
	second_dept_update, 
	third_dept, 
	brand_name, 
	prod_line, 
	imp_dt_1,
	click_dt_1,
	consumption_dt_1,
	ad_line_dt_1,
	ad_value_dt_1,
	ad_line_dt_1 / click_dt_1 AS cvr_dt_1,
	click_dt_1 / imp_dt_1 AS ctr_dt_1,
	ad_value_dt_1 / consumption_dt_1 AS roi_dt_1,
	consumption_dt_1 / click_dt_1 AS cpc_dt_1,
	((ad_line_dt_1 / click_dt_1) - (ad_line_dt_2 / click_dt_2)) / (ad_line_dt_2 / click_dt_2) AS cvr_yoy,
	(imp_dt_1 - imp_dt_2) / imp_dt_2 AS impression_yoy,
	(click_dt_1 - click_dt_2) / click_dt_2 AS click_yoy,
	(consumption_dt_1 - consumption_dt_2) / consumption_dt_2 AS consumption_yoy,
	(ad_line_dt_1 - ad_line_dt_2) / ad_line_dt_2 AS ad_line_yoy,
	(ad_value_dt_1 - ad_value_dt_2) / ad_value_dt_2 AS ad_value_yoy,
	(click_dt_1 / imp_dt_1 - click_dt_2 / imp_dt_2) / (click_dt_2 / imp_dt_2) AS ctr_yoy,
	(ad_value_dt_1 / consumption_dt_1 - ad_value_dt_2 / consumption_dt_2) / ad_value_dt_2 / consumption_dt_2 AS roi_yoy,
	(consumption_dt_1 / click_dt_1 - consumption_dt_2 / click_dt_2) / (consumption_dt_2 / click_dt_2) AS cpc_yoy
	FROM 
		b
	ORDER BY 
		consumption_dt_1 DESC;