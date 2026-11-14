
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    cart_activity_rank as unique_field,
    count(*) as n_records

from default.mart_category_cart_performance
where cart_activity_rank is not null
group by cart_activity_rank
having count(*) > 1



  
  
      
    ) dbt_internal_test