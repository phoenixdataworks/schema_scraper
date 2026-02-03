# sales.email_address

**Category:** Domain

## Definition

Base type: `varchar` NULL

## Check Constraint

```sql
CHECK (((VALUE)::text ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'::text))
```
