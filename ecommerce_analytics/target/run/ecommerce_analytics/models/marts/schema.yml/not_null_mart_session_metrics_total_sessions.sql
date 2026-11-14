
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_sessions
from default.mart_session_metrics
where total_sessions is null



  
  
      
    ) dbt_internal_test