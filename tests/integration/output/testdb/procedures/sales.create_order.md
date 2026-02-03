# sales.create_order

**Language:** plpgsql

## Parameters

| Name | Type | Direction | Default |
|------|------|-----------|---------|
| p_customer_id | integer | INPUT |  |
| p_notes | text | INPUT | NULL::text |
| p_order_id | integer | OUTPUT | NULL::integer |

## Definition

```sql
CREATE OR REPLACE PROCEDURE sales.create_order(IN p_customer_id integer, IN p_notes text DEFAULT NULL::text, INOUT p_order_id integer DEFAULT NULL::integer)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    INSERT INTO sales.orders (customer_id, notes, total_amount)
    VALUES (p_customer_id, p_notes, 0)
    RETURNING order_id INTO p_order_id;
END;
$procedure$

```
