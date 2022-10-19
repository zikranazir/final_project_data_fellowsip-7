with source as (
    select * from {{ source('sources', 'applicant_credit_final') }}
),
customers as (
    select
        ID as id,
        NAME_FAMILY_STATUS as family_status,
        CNT_FAM_MEMBERS as family_members_count,
        NAME_HOUSING_TYPE as housing_type
    from source
    limit 1000
)

select * from customers