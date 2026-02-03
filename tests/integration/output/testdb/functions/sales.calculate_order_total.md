# sales.calculate_order_total

**Type:** Scalar
**Language:** plpgsql

Calculates the total amount for an order including discounts

## Parameters

| Name | Type | Default |
|------|------|---------|
| p_order_id | integer |  |

## Returns

`numeric`

## Definition

```sql
CREATE OR REPLACE FUNCTION sales.calculate_order_total(p_order_id integer)
 RETURNS numeric
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_total DECIMAL(12,2);
BEGIN
    SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
    INTO v_total
    FROM sales.order_items
    WHERE order_id = p_order_id;

    RETURN v_total;
END;
$function$

```
