# main.customer_order_summary

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_id | INTEGER | YES |  |
| customer_name | TEXT | YES |  |
| email | TEXT | YES |  |
| total_orders | TEXT | YES |  |
| lifetime_value | TEXT | YES |  |
| last_order_date | TEXT | YES |  |

## Definition

```sql
CREATE VIEW customer_order_summary AS
SELECT
    c.customer_id,
    c.first_name || ' ' || c.last_name AS customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_value,
    MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
```
