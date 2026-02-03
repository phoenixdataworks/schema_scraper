# testdb.products

Product catalog with inventory tracking

## Statistics

- **Rows:** 0
- **Total Space:** 64 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| product_id | int | NO | IDENTITY(1,1) |  |
| sku | varchar(50) | NO |  |  |
| name | varchar(200) | NO |  |  |
| description | text | YES |  |  |
| unit_price | decimal(10,2) | NO |  |  |
| quantity_in_stock | int | NO | 0 |  |
| reorder_level | int | YES | 10 |  |
| category | varchar(100) | YES |  |  |
| is_discontinued | tinyint | YES | 0 |  |
| created_at | timestamp | YES | CURRENT_TIMESTAMP |  |

## Primary Key

**PRIMARY** (CLUSTERED)

Columns: `product_id`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| idx_products_category | BTREE | category |  |
| idx_products_sku | BTREE | sku |  |
| sku | UNIQUE BTREE | sku |  |

## Check Constraints

### chk_products_unit_price

```sql
(`unit_price` > 0)
```

### chk_products_quantity

```sql
(`quantity_in_stock` >= 0)
```

## Relationships

### Referenced By (other tables → this table)

- ← [testdb.order_items](../testdb.order_items.md) via `fk_order_items_product`
