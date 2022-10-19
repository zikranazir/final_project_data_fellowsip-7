with source as (
    select * from {{ source('sources', 'applicant_credit_final') }}
),
flags as (
    select
        ID as id,
        FLAG_OWN_CAR as flag_own_car,
        FLAG_OWN_REALTY as flag_own_realty
    from source
    limit 1000
)

select * from flags