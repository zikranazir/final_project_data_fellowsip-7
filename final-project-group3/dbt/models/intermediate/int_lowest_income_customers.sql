with occupations as (
    select * from {{ ref('stg_occupations') }}
),
lowest_income_customers as (
    select
        id,
        amount_income_total
    from
        occupations
    WHERE
        amount_income_total < 100000
    group by
        id, amount_income_total
)

select * from lowest_income_customers