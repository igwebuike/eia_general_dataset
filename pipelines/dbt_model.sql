-- dbt model for General Dataset
SELECT *, CURRENT_TIMESTAMP AS processed_at
FROM { ref('stg_general_dataset') };
