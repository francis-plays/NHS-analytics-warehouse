SELECT
    ROW_NUMBER() OVER (ORDER BY org_code) as trust_key,
    org_code,
    org_name,
    parent_org as region
FROM (
    SELECT DISTINCT 
        org_code,
        org_name,
        parent_org
    FROM {{ ref('stg_ae_attendance') }}
)
ORDER BY trust_key
