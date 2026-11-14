
    
    

select
    cart_activity_rank as unique_field,
    count(*) as n_records

from default.mart_category_cart_performance
where cart_activity_rank is not null
group by cart_activity_rank
having count(*) > 1


