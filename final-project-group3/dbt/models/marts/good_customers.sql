with customers as  (
    select * from {{ ref('stg_customers' )}}
),
have_car_realty as (
    select * from {{ ref('int_have_car_realty_customers') }}
),
highest_income as (
    select * from {{ ref('int_highest_income_customers') }}
),
completed_school as (
    select * from {{ ref('int_completed_school_customers') }}
),
not_rented_house as (
    select * from {{ ref('int_not_rented_house_customers') }}
),

good_customers as (
    select
        c.id,
        hcr.flag_own_car,
        hcr.flag_own_realty,
        hi.amount_income_total,
        cs.education_type,
        nrh.housing_type
    from customers c
    left join have_car_realty hcr USING (id)
    left join highest_income hi USING (id)
    left join completed_school cs USING (id)
    left join not_rented_house nrh USING (id)
)

select * from good_customers