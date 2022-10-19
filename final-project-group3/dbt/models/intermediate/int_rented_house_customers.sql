with family as (
    select * from {{ ref('stg_family') }}
),
rented_house_customers as (
    select
        id,
        housing_type
    from
        family
    WHERE
        housing_type = 'With parents'
    group by
        id, housing_type
)

select * from rented_house_customers