
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_events
from default.mart_event_performance
where total_events is null



  
  
      
    ) dbt_internal_test