# testdb.trg_order_items_after_update

**Table:** [testdb.order_items](../tables/testdb.order_items.md)

**Type:** AFTER

**Events:** UPDATE

## Definition

```sql
BEGIN
    UPDATE orders
    SET total_amount = calculate_order_total(NEW.order_id)
    WHERE order_id = NEW.order_id;
END
```
