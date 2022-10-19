with occupations as (
    select * from {{ ref('stg_occupations') }}
),
highest_income_customers as (
    select
        id,
        amount_income_total
    from
        occupations
    WHERE
        amount_income_total > 200000
    group by
        id, amount_income_total
)

select * from highest_income_customers