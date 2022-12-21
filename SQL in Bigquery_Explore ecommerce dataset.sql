
-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
  SUM(totals.visits) AS visits,
  SUM(totals.pageviews) AS pageviews,
  SUM(totals.transactions) AS transactions,
  SUM(totals.totalTransactionRevenue)/POWER(10,6) AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
WHERE
  _table_suffix BETWEEN '0101'AND'0331'
GROUP BY 1
ORDER BY 1

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
  trafficSource.source AS source,
  SUM(totals.visits) AS total_visits,
  SUM(totals.bounces) AS total_no_of_bounces,
  ROUND(SUM(totals.bounces)*100.0/SUM(totals.visits),8) AS bounce_rate
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY 1
ORDER BY 2 DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
WITH
  month AS(
  SELECT
    "month" AS time_type,
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS time,
    trafficSource.source AS source,
    SUM(totals.totalTransactionRevenue)/POWER(10,6) AS revenue,
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  GROUP BY 1,2,3),
  week AS (
  SELECT
    "week" AS time_type,
    FORMAT_DATE("%Y%W",PARSE_DATE("%Y%m%d",date)) AS time,
    trafficSource.source AS source,
    SUM(totals.totalTransactionRevenue)/POWER(10,6) AS revenue
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
  GROUP BY 1,2,3)
SELECT*
FROM month
UNION ALL
SELECT *
FROM week
ORDER BY source,time_type,time

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

WITH
  purchase AS (
  SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) AS avg_pageviews_purchase
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE
    _table_suffix BETWEEN '0601'AND'0731'
    AND totals.transactions >=1
  GROUP BY
    1),
  non_purchase AS (
  SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    SUM(totals.pageviews)/COUNT(DISTINCT fullVisitorId) AS avg_pageviews_non_purchase
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  WHERE
    _table_suffix BETWEEN '0601'AND'0731'
    AND totals.transactions IS NULL
  GROUP BY
    1)
SELECT
  purchase.month,
  avg_pageviews_purchase,
  avg_pageviews_non_purchase
FROM
  purchase
INNER JOIN
  non_purchase
ON
  purchase.month=non_purchase.month
ORDER BY
  purchase.month


-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
  SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) AS Avg_total_transactions_per_user
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE
  totals.transactions >=1
GROUP BY
  1

-- Query 06: Average amount of money spent per session
#standardSQL

SELECT
  FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
  ROUND(SUM(totals.totalTransactionRevenue)/COUNT(fullVisitorId),2) AS avg_revenue_by_user_per_visit
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE
  totals.transactions IS NOT NULL
GROUP BY
  1

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

SELECT
  product.v2ProductName AS other_purchased_products,
  SUM(product.productQuantity) AS quantity
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
WHERE
  fullVisitorId IN (
  SELECT
    DISTINCT fullVisitorId
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE
    product.v2ProductName="YouTube Men's Vintage Henley"
    AND product.productRevenue IS NOT NULL)
  AND product.v2ProductName <>"YouTube Men's Vintage Henley"
  AND product.productRevenue IS NOT NULL
GROUP BY
  1
ORDER BY
  1

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

WITH
  productview AS(
  SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    COUNT(product.v2ProductName) AS num_product_view
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE
    _table_suffix BETWEEN '0101'AND'0331'
    AND hits.eCommerceAction.action_type="2"
  GROUP BY 1),
  addtocart AS (
  SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    COUNT(product.v2ProductName) AS num_addtocart
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE
    _table_suffix BETWEEN '0101'AND'0331'
    AND hits.eCommerceAction.action_type="3"
  GROUP BY 1),
  purchase AS(
  SELECT
    FORMAT_DATE("%Y%m",PARSE_DATE("%Y%m%d",date)) AS month,
    COUNT(product.v2ProductName) AS num_purchase
  FROM
    `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`,
    UNNEST (hits) hits,
    UNNEST (hits.product) product
  WHERE
    _table_suffix BETWEEN '0101'AND'0331'
    AND hits.eCommerceAction.action_type="6"
  GROUP BY 1)
SELECT
  p.month,
  num_product_view,
  num_addtocart,
  num_purchase,
  ROUND(num_addtocart*100.0/num_product_view,2) AS add_to_cart_rate,
  ROUND(num_purchase*100.0/num_product_view,2) AS purchase_rate
FROM productview AS p
INNER JOIN addtocart AS a
ON p.month=a.month
INNER JOIN purchase AS p1
ON p.month=p1.month
ORDER BY p.month

