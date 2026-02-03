# TESTUSER.CUSTOMER_ORDER_SUMMARY

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| CUSTOMER_ID | NUMBER | NO |  |
| CUSTOMER_NAME | VARCHAR2 | YES |  |
| EMAIL | VARCHAR2 | NO |  |
| TOTAL_ORDERS | NUMBER | YES |  |
| LIFETIME_VALUE | NUMBER | YES |  |
| LAST_ORDER_DATE | TIMESTAMP(6) | YES |  |

## Definition

```sql
SELECT
            c.customer_id,
            c.first_name || ' ' || c.last_name AS customer_name,
            c.email,
            COUNT(o.order_id) AS total_orders,
            NVL(SUM(o.total_amount), 0) AS lifetime_value,
            MAX(o.order_date) AS last_order_date
        FROM customers c
        LEFT JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.email
```
