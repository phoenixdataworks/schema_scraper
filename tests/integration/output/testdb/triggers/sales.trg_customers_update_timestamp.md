# sales.trg_customers_update_timestamp

**Table:** [sales.customers](../tables/sales.customers.md)

**Type:** BEFORE

**Events:** UPDATE

## Definition

```sql
CREATE TRIGGER trg_customers_update_timestamp BEFORE UPDATE ON sales.customers FOR EACH ROW EXECUTE FUNCTION sales.update_timestamp()
```
