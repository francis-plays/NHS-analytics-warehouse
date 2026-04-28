SELECT
    dd.month_name,
    dd.year,
    ROUND(AVG(f.pct_seen_4hrs), 1) as avg_4hr_performance,
    SUM(f.total_attendances) as total_attendances,
    SUM(f.over_4hrs) as total_over_4hrs,
    COUNT(DISTINCT f.trust_key) as trusts_reporting
    
FROM {{ ref('fact_ae_attendance') }} f
JOIN {{ ref('dim_date') }} dd ON f.date_key = dd.date_key
GROUP BY dd.month_name, dd.year, dd.period
ORDER BY dd.period