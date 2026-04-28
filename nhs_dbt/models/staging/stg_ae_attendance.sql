SELECT
    PERIOD as period,
    ORG_CODE as org_code,
    ORG_NAME as org_name,
    PARENT_ORG as parent_org,
    TYPE1_ATTENDANCES as type1_attendances,
    TYPE1_OVER_4HRS as type1_over_4hrs,
    
    -- Calculate metrics
    (TYPE1_ATTENDANCES - TYPE1_OVER_4HRS) as seen_within_4hrs,
    
    CASE 
        WHEN TYPE1_ATTENDANCES > 0 
        THEN ROUND((TYPE1_ATTENDANCES - TYPE1_OVER_4HRS) / TYPE1_ATTENDANCES * 100, 1)
        ELSE NULL 
    END as pct_seen_4hrs

FROM {{ source('raw', 'ae_attendance_raw') }}
WHERE TYPE1_ATTENDANCES > 0  -- Remove rows with zero attendances
