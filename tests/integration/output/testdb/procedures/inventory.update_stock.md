# inventory.update_stock

**Language:** plpgsql

## Parameters

| Name | Type | Direction | Default |
|------|------|-----------|---------|
| p_product_id | integer | INPUT |  |
| p_quantity_change | integer | INPUT |  |

## Definition

```sql
CREATE OR REPLACE PROCEDURE inventory.update_stock(IN p_product_id integer, IN p_quantity_change integer)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    UPDATE inventory.products
    SET quantity_in_stock = quantity_in_stock + p_quantity_change
    WHERE product_id = p_product_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Product % not found', p_product_id;
    END IF;
END;
$procedure$

```
