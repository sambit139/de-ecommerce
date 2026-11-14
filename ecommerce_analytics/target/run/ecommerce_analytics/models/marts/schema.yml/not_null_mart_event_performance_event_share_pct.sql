
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_share_pct
from default.mart_event_performance
where event_share_pct is null



  
  
      
    ) dbt_internal_test