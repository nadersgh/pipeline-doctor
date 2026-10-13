with source as (
    select * from {{ source('raw', 'payments') }}
),

renamed as (
    select
        id as payment_id,
        order_id,
        payment_method,
        cast(amount as integer) / 100.0 as amount,
        _loaded_at
    from source
)

select * from renamed
