# main.orders

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| order_id | INTEGER | YES | IDENTITY(1,1) |  |
| customer_id | INTEGER | NO |  |  |
| order_date | TEXT | YES | datetime('now') |  |
| status | TEXT | YES | 'pending' |  |
| total_amount | REAL | NO | 0 |  |
| notes | TEXT | YES |  |  |

## Primary Key

**pk_orders** (CLUSTERED)

Columns: `order_id`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| fk_orders_0 | customer_id | main.customers(customer_id) | NO ACTION | NO ACTION |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| idx_orders_status | BTREE | status |  |
| idx_orders_date | BTREE | order_date |  |
| idx_orders_customer | BTREE | customer_id |  |

## Relationships

### References (this table → other tables)

- → [main.customers](../main.customers.md) via `fk_orders_0`

### Referenced By (other tables → this table)

- ← [main.order_items](../main.order_items.md) via `fk_order_items_1`
