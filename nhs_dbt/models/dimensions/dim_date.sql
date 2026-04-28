SELECT
    ROW_NUMBER() OVER (ORDER BY period) as date_key,
    period,
    
    -- Extract month name from period (e.g., "MSitAE-OCTOBER-2025" -> "OCTOBER")
    SPLIT_PART(period, '-', 2) as month_name,
    
    -- Extract year (e.g., "MSitAE-OCTOBER-2025" -> "2025")
    SPLIT_PART(period, '-', 3) as year
    
FROM (
    SELECT DISTINCT period
    FROM {{ ref('stg_ae_attendance') }}
)
ORDER BY date_key