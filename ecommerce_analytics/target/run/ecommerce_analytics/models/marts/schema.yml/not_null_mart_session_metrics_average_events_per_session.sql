
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select average_events_per_session
from default.mart_session_metrics
where average_events_per_session is null



  
  
      
    ) dbt_internal_test