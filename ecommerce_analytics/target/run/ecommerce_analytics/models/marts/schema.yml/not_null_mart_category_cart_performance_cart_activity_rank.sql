
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cart_activity_rank
from default.mart_category_cart_performance
where cart_activity_rank is null



  
  
      
    ) dbt_internal_test