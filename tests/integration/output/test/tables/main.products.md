# main.products

## Statistics

- **Rows:** 3
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| product_id | INTEGER | YES | IDENTITY(1,1) |  |
| sku | TEXT | NO |  |  |
| name | TEXT | NO |  |  |
| description | TEXT | YES |  |  |
| unit_price | REAL | NO |  |  |
| quantity_in_stock | INTEGER | NO | 0 |  |
| reorder_level | INTEGER | YES | 10 |  |
| category | TEXT | YES |  |  |
| is_discontinued | INTEGER | YES | 0 |  |
| created_at | TEXT | YES | datetime('now') |  |

## Primary Key

**pk_products** (CLUSTERED)

Columns: `product_id`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| idx_products_sku | BTREE | sku |  |
| idx_products_category | BTREE | category |  |
| sqlite_autoindex_products_1 | UNIQUE BTREE | sku |  |

## Relationships

### Referenced By (other tables → this table)

- ← [main.order_items](../main.order_items.md) via `fk_order_items_0`
