-- =====================================================
-- Fleet Analytics: Example Analytics Queries
-- =====================================================


-- 1. Vehicles with the highest number of maintenance events
-- Useful for identifying high-maintenance or problematic vehicles
SELECT
    dc.car_id,
    dc.registration_number,
    fms.maintenance_count
FROM fact_maintenance_summary fms
JOIN dim_car dc
    ON fms.car_id = dc.car_id
ORDER BY fms.maintenance_count DESC
LIMIT 10;



-- 2. Open issues by vehicle model
-- Helps identify models with reliability problems
SELECT
    dm.model_name,
    COUNT(*) AS open_issue_count
FROM fact_active_issues fai
JOIN dim_car dc
    ON fai.car_id = dc.car_id
JOIN dim_model dm
    ON dc.model_id = dm.model_id
GROUP BY dm.model_name
ORDER BY open_issue_count DESC;



-- 3. Issue severity distribution
-- Useful for risk and operations prioritization
SELECT
    severity,
    COUNT(*) AS issue_count
FROM fact_active_issues
GROUP BY severity
ORDER BY issue_count DESC;



-- 4. Vehicles with the highest average maintenance cost
-- Identifies vehicles with the highest cost impact
SELECT
    dc.registration_number,
    fms.avg_cost
FROM fact_maintenance_summary fms
JOIN dim_car dc
    ON fms.car_id = dc.car_id
ORDER BY fms.avg_cost DESC
LIMIT 10;



-- 5. Number of active issues per vehicle
-- Highlights vehicles that may require immediate attention
SELECT
    dc.registration_number,
    COUNT(*) AS active_issue_count
FROM fact_active_issues fai
JOIN dim_car dc
    ON fai.car_id = dc.car_id
GROUP BY dc.registration_number
ORDER BY active_issue_count DESC
LIMIT 10;



-- 6. Average maintenance cost by vehicle model
-- Useful for cost comparisons across models
SELECT
    dm.model_name,
    ROUND(AVG(fms.avg_cost), 2) AS avg_maintenance_cost
FROM fact_maintenance_summary fms
JOIN dim_car dc
    ON fms.car_id = dc.car_id
JOIN dim_model dm
    ON dc.model_id = dm.model_id
GROUP BY dm.model_name
ORDER BY avg_maintenance_cost DESC;