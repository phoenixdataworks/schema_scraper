# inventory.get_products_by_category

**Type:** Table
**Language:** plpgsql

## Parameters

| Name | Type | Default |
|------|------|---------|
| p_category | character varying |  |

## Return Columns

| Column | Type | Nullable |
|--------|------|----------|
| product_id | integer | YES |
| sku | character varying | YES |
| name | character varying | YES |
| unit_price | numeric | YES |
| quantity_in_stock | integer | YES |

## Definition

```sql
CREATE OR REPLACE FUNCTION inventory.get_products_by_category(p_category character varying)
 RETURNS TABLE(product_id integer, sku character varying, name character varying, unit_price numeric, quantity_in_stock integer)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY
    SELECT p.product_id, p.sku, p.name, p.unit_price, p.quantity_in_stock
    FROM inventory.products p
    WHERE p.category = p_category
      AND p.is_discontinued = FALSE
    ORDER BY p.name;
END;
$function$

```
