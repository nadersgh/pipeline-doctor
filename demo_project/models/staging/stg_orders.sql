with source as (
    select * from {{ source('raw', 'orders') }}
),

renamed as (
    select
        id as order_id,
        customer_id,
        cast(order_date as date) as order_date,
        status,
        _loaded_at
    from source
)

select * from renamed
