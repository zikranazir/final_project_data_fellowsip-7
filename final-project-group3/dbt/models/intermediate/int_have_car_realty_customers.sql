with flags as (
    select * from {{ ref('stg_flags') }}
),
have_car_realty_customers as (
    select
        id,
        flag_own_car,
        flag_own_realty
    from
        flags
    WHERE
        flag_own_car = 'Y' OR flag_own_realty = 'Y'
    group by
        id, flag_own_car, flag_own_realty
)

select * from have_car_realty_customers