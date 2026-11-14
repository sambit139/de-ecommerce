
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select subcategory
from default.mart_category_cart_performance
where subcategory is null



  
  
      
    ) dbt_internal_test