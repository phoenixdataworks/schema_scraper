# testdb.customers

Customer master table containing contact and shipping information

## Statistics

- **Rows:** 2
- **Total Space:** 64 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| customer_id | int | NO | IDENTITY(1,1) |  |
| email | varchar(255) | NO |  |  |
| first_name | varchar(50) | NO |  |  |
| last_name | varchar(50) | NO |  |  |
| phone | varchar(20) | YES |  |  |
| street | varchar(200) | YES |  |  |
| city | varchar(100) | YES |  |  |
| state | varchar(50) | YES |  |  |
| postal_code | varchar(20) | YES |  |  |
| country | varchar(50) | YES | USA |  |
| created_at | timestamp | YES | CURRENT_TIMESTAMP |  |
| updated_at | timestamp | YES | CURRENT_TIMESTAMP |  |
| is_active | tinyint | YES | 1 |  |

## Primary Key

**PRIMARY** (CLUSTERED)

Columns: `customer_id`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| email | UNIQUE BTREE | email |  |
| idx_customers_email | BTREE | email |  |
| idx_customers_name | BTREE | last_name, first_name |  |

## Relationships

### Referenced By (other tables → this table)

- ← [testdb.orders](../testdb.orders.md) via `fk_orders_customer`
