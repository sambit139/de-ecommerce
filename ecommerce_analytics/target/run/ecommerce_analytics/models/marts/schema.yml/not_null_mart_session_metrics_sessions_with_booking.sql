
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select sessions_with_booking
from default.mart_session_metrics
where sessions_with_booking is null



  
  
      
    ) dbt_internal_test