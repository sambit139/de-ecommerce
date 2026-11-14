
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_sessions
from default.mart_event_performance
where unique_sessions is null



  
  
      
    ) dbt_internal_test