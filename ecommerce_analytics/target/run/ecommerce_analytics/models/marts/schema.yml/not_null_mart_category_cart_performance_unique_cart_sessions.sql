
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select unique_cart_sessions
from default.mart_category_cart_performance
where unique_cart_sessions is null



  
  
      
    ) dbt_internal_test