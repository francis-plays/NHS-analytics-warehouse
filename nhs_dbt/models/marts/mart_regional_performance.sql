SELECT
    dt.region,
    ROUND(AVG(f.pct_seen_4hrs), 1) as avg_4hr_performance,
    COUNT(DISTINCT f.trust_key) as num_trusts,
    SUM(f.total_attendances) as total_attendances,
    SUM(f.over_4hrs) as total_over_4hrs
    
FROM {{ ref('fact_ae_attendance') }} f
JOIN {{ ref('dim_trust') }} dt ON f.trust_key = dt.trust_key
GROUP BY dt.region
ORDER BY avg_4hr_performance ASC