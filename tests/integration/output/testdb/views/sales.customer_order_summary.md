# sales.customer_order_summary

Aggregated customer statistics including order counts and lifetime value

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_id | integer | YES |  |
| customer_name | text | YES |  |
| email | character varying(255) | YES |  |
| total_orders | bigint | YES |  |
| lifetime_value | numeric | YES |  |
| last_order_date | timestamp with time zone | YES |  |

## Definition

```sql
 SELECT c.customer_id,
    (((c.first_name)::text || ' '::text) || (c.last_name)::text) AS customer_name,
    c.email,
    count(o.order_id) AS total_orders,
    COALESCE(sum(o.total_amount), (0)::numeric) AS lifetime_value,
    max(o.order_date) AS last_order_date
   FROM (sales.customers c
     LEFT JOIN sales.orders o ON ((c.customer_id = o.customer_id)))
  GROUP BY c.customer_id, c.first_name, c.last_name, c.email;
```
