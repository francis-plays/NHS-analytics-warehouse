SELECT
    ROW_NUMBER() OVER (ORDER BY dt.trust_key, dd.date_key) as attendance_key,
    dt.trust_key,
    dd.date_key,
    s.type1_attendances as total_attendances,
    s.type1_over_4hrs as over_4hrs,
    s.seen_within_4hrs,
    s.pct_seen_4hrs
    
FROM {{ ref('stg_ae_attendance') }} s
JOIN {{ ref('dim_trust') }} dt 
    ON s.org_code = dt.org_code
JOIN {{ ref('dim_date') }} dd 
    ON s.period = dd.period