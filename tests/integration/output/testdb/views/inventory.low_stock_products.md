# inventory.low_stock_products

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_id | integer | YES |  |
| sku | character varying(50) | YES |  |
| name | character varying(200) | YES |  |
| quantity_in_stock | integer | YES |  |
| reorder_level | integer | YES |  |
| units_to_order | integer | YES |  |

## Definition

```sql
 SELECT product_id,
    sku,
    name,
    quantity_in_stock,
    reorder_level,
    (reorder_level - quantity_in_stock) AS units_to_order
   FROM inventory.products
  WHERE ((quantity_in_stock < reorder_level) AND (is_discontinued = false));
```
