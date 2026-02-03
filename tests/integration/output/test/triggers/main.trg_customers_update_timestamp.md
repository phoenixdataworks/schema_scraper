# main.trg_customers_update_timestamp

**Table:** [main.customers](../tables/main.customers.md)

**Type:** AFTER

**Events:** UPDATE

## Definition

```sql
CREATE TRIGGER trg_customers_update_timestamp
AFTER UPDATE ON customers
FOR EACH ROW
BEGIN
    UPDATE customers SET updated_at = datetime('now') WHERE customer_id = NEW.customer_id;
END
```
