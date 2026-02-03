# sales.recalculate_order_total

**Type:** Scalar
**Language:** plpgsql

## Returns

`trigger`

## Definition

```sql
CREATE OR REPLACE FUNCTION sales.recalculate_order_total()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    UPDATE sales.orders
    SET total_amount = sales.calculate_order_total(
        CASE WHEN TG_OP = 'DELETE' THEN OLD.order_id ELSE NEW.order_id END
    )
    WHERE order_id = CASE WHEN TG_OP = 'DELETE' THEN OLD.order_id ELSE NEW.order_id END;

    RETURN NULL;
END;
$function$

```
