
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select observed_cart_item_price_sum
from default.mart_category_cart_performance
where observed_cart_item_price_sum is null



  
  
      
    ) dbt_internal_test