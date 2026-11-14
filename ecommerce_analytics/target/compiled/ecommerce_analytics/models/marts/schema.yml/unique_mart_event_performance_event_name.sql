
    
    

select
    event_name as unique_field,
    count(*) as n_records

from default.mart_event_performance
where event_name is not null
group by event_name
having count(*) > 1


