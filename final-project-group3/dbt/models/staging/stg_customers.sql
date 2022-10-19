{{ config(
    cluster_by=['education_type'])
}}

with source as (
    select * from {{ source('sources', 'applicant_credit_final') }}
),
customers as (
    select
        ID as id,
        CODE_GENDER as gender,
        Age as age,
        NAME_EDUCATION_TYPE as education_type,
    from source
    limit 1000
)

select * from customers