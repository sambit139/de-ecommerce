
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cart_events
from default.mart_category_cart_performance
where cart_events is null



  
  
      
    ) dbt_internal_test