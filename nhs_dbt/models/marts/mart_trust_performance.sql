SELECT
    dt.org_name as trust_name,
    dt.region,
    ROUND(AVG(f.pct_seen_4hrs), 1) as avg_4hr_performance,
    SUM(f.total_attendances) as total_attendances,
    SUM(f.over_4hrs) as total_over_4hrs,
    CASE 
        WHEN AVG(f.pct_seen_4hrs) >= 95 THEN 'PASS'
        ELSE 'FAIL'
    END as target_status
    
FROM {{ ref('fact_ae_attendance') }} f
JOIN {{ ref('dim_trust') }} dt ON f.trust_key = dt.trust_key
GROUP BY dt.org_name, dt.region
ORDER BY avg_4hr_performance ASC