# main.customers

## Statistics

- **Rows:** 2
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| customer_id | INTEGER | YES | IDENTITY(1,1) |  |
| email | TEXT | NO |  |  |
| first_name | TEXT | NO |  |  |
| last_name | TEXT | NO |  |  |
| phone | TEXT | YES |  |  |
| street | TEXT | YES |  |  |
| city | TEXT | YES |  |  |
| state | TEXT | YES |  |  |
| postal_code | TEXT | YES |  |  |
| country | TEXT | YES | 'USA' |  |
| created_at | TEXT | YES | datetime('now') |  |
| updated_at | TEXT | YES | datetime('now') |  |
| is_active | INTEGER | YES | 1 |  |

## Primary Key

**pk_customers** (CLUSTERED)

Columns: `customer_id`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| idx_customers_email | BTREE | email |  |
| idx_customers_name | BTREE | last_name, first_name |  |
| sqlite_autoindex_customers_1 | UNIQUE BTREE | email |  |

## Relationships

### Referenced By (other tables → this table)

- ← [main.orders](../main.orders.md) via `fk_orders_0`
