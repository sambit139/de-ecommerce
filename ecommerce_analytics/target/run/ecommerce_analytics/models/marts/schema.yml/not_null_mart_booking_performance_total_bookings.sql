
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_bookings
from default.mart_booking_performance
where total_bookings is null



  
  
      
    ) dbt_internal_test