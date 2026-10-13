with source as (
    select * from {{ source('raw', 'customers') }}
),

renamed as (
    select
        id as customer_id,
        first_name,
        last_name,
        email,
        cast(created_at as timestamp) as created_at,
        _loaded_at
    from source
)

select * from renamed
