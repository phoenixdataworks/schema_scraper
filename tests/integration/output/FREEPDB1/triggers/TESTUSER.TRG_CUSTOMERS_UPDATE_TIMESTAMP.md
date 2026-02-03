# TESTUSER.TRG_CUSTOMERS_UPDATE_TIMESTAMP

**Table:** [TESTUSER.CUSTOMERS](../tables/TESTUSER.CUSTOMERS.md)

**Type:** BEFORE

**Events:** UPDATE

## Definition

```sql
BEGIN
    :NEW.updated_at := CURRENT_TIMESTAMP;
END;
```
