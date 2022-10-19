with customers as (
    select * from {{ ref('stg_customers') }}
),
productive_age_customers as (
    select
        id,
        age
    from
        customers
    WHERE
        age > 20 AND age < 50
    group by
        id, age
)

select * from productive_age_customers