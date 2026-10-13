with orders as (
    select * from {{ ref('stg_orders') }}
),

order_payments as (
    select * from {{ ref('int_order_payments') }}
),

final as (
    select
        orders.order_id,
        orders.customer_id,
        orders.order_date,
        orders.status,
        coalesce(order_payments.total_amount, 0) as amount,
        coalesce(order_payments.payment_count, 0) as payment_count,
        coalesce(order_payments.credit_card_amount, 0) as credit_card_amount,
        coalesce(order_payments.coupon_amount, 0) as coupon_amount
    from orders
    left join order_payments
        on orders.order_id = order_payments.order_id
)

select * from final
