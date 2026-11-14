
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select average_cart_event_item_price
from default.mart_category_cart_performance
where average_cart_event_item_price is null



  
  
      
    ) dbt_internal_test