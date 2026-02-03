# sales.monthly_sales_summary

*This is a materialized view.*

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|

## Definition

```sql
 SELECT date_trunc('month'::text, order_date) AS month,
    count(*) AS order_count,
    sum(total_amount) AS total_revenue,
    avg(total_amount) AS avg_order_value
   FROM sales.orders o
  WHERE (status <> 'cancelled'::sales.order_status)
  GROUP BY (date_trunc('month'::text, order_date));
```
