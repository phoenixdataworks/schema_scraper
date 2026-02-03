# sales.trg_order_items_update_total

**Table:** [sales.order_items](../tables/sales.order_items.md)

**Type:** AFTER

**Events:** INSERT, UPDATE, DELETE

## Definition

```sql
CREATE TRIGGER trg_order_items_update_total AFTER INSERT OR DELETE OR UPDATE ON sales.order_items FOR EACH ROW EXECUTE FUNCTION sales.recalculate_order_total()
```
