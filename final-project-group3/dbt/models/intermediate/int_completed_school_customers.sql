with customers as (
    select * from {{ ref('stg_customers') }}
),
completed_school_customers as (
    select
        id,
        education_type
    from
        customers
    WHERE
        education_type != 'secondary'
    group by
        id, education_type
)

select * from completed_school_customers