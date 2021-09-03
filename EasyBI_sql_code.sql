WITH
	a AS
	(
		SELECT
			second_dept_update,
			third_dept,
			(
				CASE
					WHEN prod_line IN('CPA-京任务', 'CPS-京挑客', '京东展位', '京东快车', '京东直投-新媒体', '京东直投-腾讯', '海投广告', '购物触点')
					THEN 'RTB广告'
					WHEN prod_line IN('CPD-合约展位', '京腾合约', '京条合约')
					THEN '合约广告'
					ELSE '其他'
				END) AS ad_type,
			consumption AS consumption_dt_1
		FROM
			wangrui_test_view
		WHERE
			dt >= #dt_1#
			AND dt <= #dt_1_end#
			AND #{filter|field=second_dept_update}
	)
	,
	a_1 AS
	(
		SELECT
			second_dept_update,
			third_dept,
			(
				CASE
					WHEN prod_line IN('CPA-京任务', 'CPS-京挑客', '京东展位', '京东快车', '京东直投-新媒体', '京东直投-腾讯', '海投广告', '购物触点')
					THEN 'RTB广告'
					WHEN prod_line IN('CPD-合约展位', '京腾合约', '京条合约')
					THEN '合约广告'
					ELSE '其他'
				END) AS ad_type,
			consumption AS consumption_dt_2
		FROM
			wangrui_test_view
		WHERE
			dt >= #dt_2#
			AND dt <= #dt_2_end#
			AND #{filter|field=second_dept_update}
	)
	,
	b AS
	(
		SELECT
			a.second_dept_update,
			a.third_dept,
			a.ad_type,
			SUM(a.consumption_dt_1) AS consumption_dt_1,
			SUM(a_1.consumption_dt_2) AS consumption_dt_2
		FROM
			a
		LEFT OUTER JOIN a_1
		ON
			a.second_dept_update = a_1.second_dept_update
			AND a.third_dept = a_1.third_dept
			AND a.ad_type = a_1.ad_type
		GROUP BY
			a.second_dept_update,
			a.third_dept,
			a.ad_type
	)
SELECT
	second_dept_update,
	third_dept,
	ad_type,
	consumption_dt_1,
	consumption_dt_2
FROM
	b
WHERE #{filter|field=ad_type}
ORDER BY consumption_dt_1;