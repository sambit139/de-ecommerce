
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select articletype
from default.mart_category_cart_performance
where articletype is null



  
  
      
    ) dbt_internal_test