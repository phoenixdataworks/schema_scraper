# TESTUSER.LOW_STOCK_PRODUCTS

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| PRODUCT_ID | NUMBER | NO |  |
| SKU | VARCHAR2 | NO |  |
| NAME | VARCHAR2 | NO |  |
| QUANTITY_IN_STOCK | NUMBER | NO |  |
| REORDER_LEVEL | NUMBER | YES |  |
| UNITS_TO_ORDER | NUMBER | YES |  |

## Definition

```sql
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
