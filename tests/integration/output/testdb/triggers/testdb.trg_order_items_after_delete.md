# testdb.trg_order_items_after_delete

**Table:** [testdb.order_items](../tables/testdb.order_items.md)

**Type:** AFTER

**Events:** DELETE

## Definition

```sql
BEGIN
    UPDATE orders
    SET total_amount = calculate_order_total(OLD.order_id)
    WHERE order_id = OLD.order_id;
END
```
