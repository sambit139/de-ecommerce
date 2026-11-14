
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_booking_sessions
from default.mart_booking_performance
where unique_booking_sessions is null



  
  
      
    ) dbt_internal_test