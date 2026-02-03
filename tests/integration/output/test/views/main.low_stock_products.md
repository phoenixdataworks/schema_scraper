# main.low_stock_products

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_id | INTEGER | YES |  |
| sku | TEXT | YES |  |
| name | TEXT | YES |  |
| quantity_in_stock | INTEGER | YES |  |
| reorder_level | INTEGER | YES |  |
| units_to_order | TEXT | YES |  |

## Definition

```sql
CREATE VIEW low_stock_products AS
SELECT
    product_id,
    sku,
    name,
    quantity_in_stock,
    reorder_level,
    reorder_level - quantity_in_stock AS units_to_order
FROM products
WHERE quantity_in_stock < reorder_level
  AND is_discontinued = 0
```
