with customers as  (
    select * from {{ ref('stg_customers' )}}
),
lowest_income as (
    select * from {{ ref('int_lowest_income_customers') }}
),
incomplete_school as (
    select * from {{ ref('int_incomplete_school_customers') }}
),
rented_house as (
    select * from {{ ref('int_rented_house_customers') }}
),

bad_customers as (
    select
        c.id,
        li.amount_income_total,
        ins.education_type,
        rh.housing_type
    from customers c
    left join lowest_income li USING (id)
    left join incomplete_school ins USING (id)
    left join rented_house rh USING (id)
)

select * from bad_customers