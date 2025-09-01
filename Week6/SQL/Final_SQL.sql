USE dwh;

-- =========================
-- Key Metrics (scorecards)
-- =========================
CREATE OR REPLACE VIEW v_kpis AS
SELECT
  COUNT(DISTINCT f.orderid)                           AS total_orders,
  ROUND(SUM(f.paymentvalue), 2)                       AS total_revenue,
  ROUND(AVG(p.paymentinstallments), 2)                AS avg_installments,
  COUNT(p.paymentid)                                  AS total_payments
FROM fact_sales f
LEFT JOIN dim_payments p ON f.paymentid = p.paymentid;

-- ==========================================
-- Seasonal Patterns (by month + by season)
-- ==========================================
CREATE OR REPLACE VIEW v_orders_by_month AS
SELECT
  DATE_FORMAT(o.orderdate, '%Y-%m') AS ym,
  COUNT(DISTINCT f.orderid)         AS orders,
  ROUND(SUM(f.paymentvalue), 2)     AS revenue
FROM fact_sales f
JOIN dim_orders o ON f.orderid = o.orderid
WHERE o.orderdate IS NOT NULL
GROUP BY ym
ORDER BY ym;

CREATE OR REPLACE VIEW v_orders_by_season AS
SELECT
  CASE
    WHEN MONTH(o.orderdate) IN (12,1,2) THEN 'Winter'
    WHEN MONTH(o.orderdate) IN (3,4,5)  THEN 'Spring'
    WHEN MONTH(o.orderdate) IN (6,7,8)  THEN 'Summer'
    ELSE 'Fall'
  END AS season,
  COUNT(DISTINCT f.orderid)     AS orders,
  ROUND(SUM(f.paymentvalue), 2) AS revenue
FROM fact_sales f
JOIN dim_orders o ON f.orderid = o.orderid
WHERE o.orderdate IS NOT NULL
GROUP BY season
ORDER BY orders DESC;

-- ==========================================
-- Order Timing (hour of day)
-- ==========================================
CREATE OR REPLACE VIEW v_orders_by_hour AS
SELECT
  HOUR(o.orderdate)              AS order_hour,
  COUNT(DISTINCT f.orderid)      AS orders
FROM fact_sales f
JOIN dim_orders o ON f.orderid = o.orderid
WHERE o.orderdate IS NOT NULL
GROUP BY order_hour
ORDER BY order_hour;

-- ==========================================
-- Payment Preferences (top methods)
-- ==========================================
CREATE OR REPLACE VIEW v_payment_preferences AS
SELECT
  p.paymenttype,
  COUNT(*) AS payments
FROM fact_sales f
JOIN dim_payments p ON f.paymentid = p.paymentid
GROUP BY p.paymenttype
ORDER BY payments DESC;

-- ==========================================
-- Installment Patterns (overall & by type)
-- ==========================================
CREATE OR REPLACE VIEW v_avg_installments AS
SELECT
  ROUND(AVG(p.paymentinstallments), 2) AS avg_installments
FROM dim_payments p
WHERE p.paymentinstallments IS NOT NULL;

CREATE OR REPLACE VIEW v_avg_installments_by_type AS
SELECT
  p.paymenttype,
  ROUND(AVG(p.paymentinstallments), 2) AS avg_installments
FROM dim_payments p
WHERE p.paymentinstallments IS NOT NULL
GROUP BY p.paymenttype
ORDER BY avg_installments DESC;

-- ==========================================
-- Geographic Analysis (purchase frequency)
-- ==========================================
CREATE OR REPLACE VIEW v_purchases_by_state AS
SELECT
  u.userstate              AS state,
  COUNT(DISTINCT f.orderid) AS orders,
  ROUND(SUM(f.paymentvalue), 2) AS revenue
FROM fact_sales f
JOIN dim_users u ON f.userid = u.userid
GROUP BY u.userstate
ORDER BY orders DESC;

-- ==========================================
-- Logistics: routes with heaviest traffic
-- ==========================================
CREATE OR REPLACE VIEW v_routes_traffic AS
SELECT
  s.sellerstate  AS origin_state,
  u.userstate    AS dest_state,
  COUNT(DISTINCT f.orderid) AS orders
FROM fact_sales f
JOIN dim_sellers s ON f.sellerid = s.sellerid
JOIN dim_users   u ON f.userid   = u.userid
GROUP BY origin_state, dest_state
ORDER BY orders DESC;

-- ==========================================
-- Delivery delays by route (longest delays)
-- ==========================================
CREATE OR REPLACE VIEW v_routes_delay AS
SELECT
  s.sellerstate  AS origin_state,
  u.userstate    AS dest_state,
  ROUND(AVG(DATEDIFF(o.delivereddate, o.estimatedtimedelivery)), 2) AS avg_delay_days,
  COUNT(DISTINCT CASE WHEN o.delivereddate > o.estimatedtimedelivery THEN o.orderid END) AS late_orders,
  COUNT(DISTINCT o.orderid) AS total_orders
FROM fact_sales f
JOIN dim_orders  o ON f.orderid  = o.orderid
JOIN dim_users   u ON f.userid   = u.userid
JOIN dim_sellers s ON f.sellerid = s.sellerid
WHERE o.delivereddate IS NOT NULL
  AND o.estimatedtimedelivery IS NOT NULL
GROUP BY origin_state, dest_state
HAVING total_orders > 0
ORDER BY avg_delay_days DESC;

-- ==========================================
-- Delivery Performance: count of late orders
-- ==========================================
CREATE OR REPLACE VIEW v_late_orders AS
SELECT
  COUNT(*) AS late_orders
FROM dim_orders o
WHERE o.delivereddate IS NOT NULL
  AND o.estimatedtimedelivery IS NOT NULL
  AND o.delivereddate > o.estimatedtimedelivery;

-- ==========================================
-- Delivery Performance: delay vs feedback corr
-- ==========================================
CREATE OR REPLACE VIEW v_delay_feedback_corr AS
SELECT
  (SUM(dx*fs) - COUNT(*)*AVG(dx)*AVG(fs)) /
  NULLIF(
    SQRT((SUM(dx*dx) - COUNT(*)*POW(AVG(dx),2)) * (SUM(fs*fs) - COUNT(*)*POW(AVG(fs),2))),
    0
  ) AS corr
FROM (
  SELECT
    TIMESTAMPDIFF(HOUR, o.estimatedtimedelivery, o.delivereddate) AS dx,
    fdb.feedbackscore AS fs
  FROM dim_orders   o
  JOIN dim_feedback fdb ON o.orderid = fdb.orderid
  WHERE o.delivereddate IS NOT NULL
    AND o.estimatedtimedelivery IS NOT NULL
    AND fdb.feedbackscore IS NOT NULL
) t;

-- ==========================================
-- Avg shipping time by destination state
-- ==========================================
CREATE OR REPLACE VIEW v_avg_shipping_time_by_state AS
SELECT
  u.userstate AS dest_state,
  ROUND(AVG(TIMESTAMPDIFF(HOUR, o.orderdate, o.delivereddate))/24, 2) AS avg_ship_days,
  COUNT(*) AS shipments
FROM fact_sales f
JOIN dim_orders o  ON f.orderid = o.orderid
JOIN dim_users  u  ON f.userid  = u.userid
WHERE o.orderdate IS NOT NULL
  AND o.delivereddate IS NOT NULL
GROUP BY dest_state
ORDER BY avg_ship_days DESC;

-- ==========================================
-- Avg shipping time by route
-- ==========================================
CREATE OR REPLACE VIEW v_avg_shipping_time_by_route AS
SELECT
  s.sellerstate AS origin_state,
  u.userstate   AS dest_state,
  ROUND(AVG(TIMESTAMPDIFF(HOUR, o.orderdate, o.delivereddate))/24, 2) AS avg_ship_days,
  COUNT(*) AS shipments
FROM fact_sales f
JOIN dim_orders  o ON f.orderid  = o.orderid
JOIN dim_users   u ON f.userid   = u.userid
JOIN dim_sellers s ON f.sellerid = s.sellerid
WHERE o.orderdate IS NOT NULL
  AND o.delivereddate IS NOT NULL
GROUP BY origin_state, dest_state
ORDER BY avg_ship_days DESC;

-- ==========================================
-- Avg difference between estimated and actual
-- ==========================================
CREATE OR REPLACE VIEW v_avg_eta_vs_actual_diff AS
SELECT
  ROUND(AVG(TIMESTAMPDIFF(HOUR, o.estimatedtimedelivery, o.delivereddate)), 2) AS avg_diff_hours,
  ROUND(AVG(ABS(TIMESTAMPDIFF(HOUR, o.estimatedtimedelivery, o.delivereddate))), 2) AS avg_abs_diff_hours,
  SUM(CASE WHEN o.delivereddate > o.estimatedtimedelivery THEN 1 ELSE 0 END) AS late_orders,
  COUNT(*) AS total_with_eta_and_actual
FROM dim_orders o
WHERE o.delivereddate IS NOT NULL
  AND o.estimatedtimedelivery IS NOT NULL;
