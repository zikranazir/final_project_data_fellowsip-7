with source as (
    select * from {{ source('sources', 'applicant_credit_final') }}
),
occupations as (
    select
        ID as id,
        OCCUPATION_TYPE as occupation_type,
        YEAR_WORKED as year_worked,
        NAME_INCOME_TYPE as income_type,
        AMT_INCOME_TOTAL as amount_income_total
    from source
    limit 1000
)

select * from occupations