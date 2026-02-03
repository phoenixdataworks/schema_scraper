# testdb.orders

Customer orders with status tracking

## Statistics

- **Rows:** 0
- **Total Space:** 64 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| order_id | int | NO | IDENTITY(1,1) |  |
| customer_id | int | NO |  |  |
| order_date | timestamp | YES | CURRENT_TIMESTAMP |  |
| status | enum | YES | pending |  |
| total_amount | decimal(12,2) | NO | 0.00 |  |
| notes | text | YES |  |  |

## Primary Key

**PRIMARY** (CLUSTERED)

Columns: `order_id`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| fk_orders_customer | customer_id | testdb.customers(customer_id) | RESTRICT | CASCADE |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| idx_orders_customer | BTREE | customer_id |  |
| idx_orders_date | BTREE | order_date |  |
| idx_orders_status | BTREE | status |  |

## Check Constraints

### chk_total_amount

```sql
(`total_amount` >= 0)
```

## Relationships

### References (this table → other tables)

- → [testdb.customers](../testdb.customers.md) via `fk_orders_customer`

### Referenced By (other tables → this table)

- ← [testdb.order_items](../testdb.order_items.md) via `fk_order_items_order`
