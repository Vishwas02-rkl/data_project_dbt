{{config(materialized='ephemeral')}}
SELECT 
DISTINCT
employee_id,
employee_first_name,
employee_last_name,
employee_email,
salary,
store_id,
employee_created_timestamp,
employee_updated_timestamp,
employee_is_active, 
job_title,
current_timestamp() as employee_gold_processed_at
from {{ref ('obt_b')}}
