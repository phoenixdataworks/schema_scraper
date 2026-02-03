# main.trg_order_items_delete_update_total

**Table:** [main.order_items](../tables/main.order_items.md)

**Type:** AFTER

**Events:** UPDATE, DELETE

## Definition

```sql
CREATE TRIGGER trg_order_items_delete_update_total
AFTER DELETE ON order_items
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = (
        SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
        FROM order_items
        WHERE order_id = OLD.order_id
    )
    WHERE order_id = OLD.order_id;
END
```
