# TESTUSER.ORDERS

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| ORDER_ID | NUMBER | NO | "TESTUSER"."ORDER_SEQ"."NEXTVAL" |  |
| CUSTOMER_ID | NUMBER | NO |  |  |
| ORDER_DATE | TIMESTAMP(6) | YES | CURRENT_TIMESTAMP |  |
| STATUS | VARCHAR2 | YES | 'pending' |  |
| TOTAL_AMOUNT | NUMBER | NO | 0 |  |
| NOTES | CLOB | YES |  |  |

## Primary Key

**SYS_C008774** (NONCLUSTERED)

Columns: `ORDER_ID`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| SYS_C008775 | CUSTOMER_ID | TESTUSER.CUSTOMERS(CUSTOMER_ID) | NO ACTION | NO ACTION |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| IDX_ORDERS_CUSTOMER | NORMAL | CUSTOMER_ID |  |
| IDX_ORDERS_DATE | FUNCTION-BASED NORMAL | SYS_NC00007$ |  |
| IDX_ORDERS_STATUS | NORMAL | STATUS |  |

## Relationships

### References (this table → other tables)

- → [TESTUSER.CUSTOMERS](../TESTUSER.CUSTOMERS.md) via `SYS_C008775`

### Referenced By (other tables → this table)

- ← [TESTUSER.ORDER_ITEMS](../TESTUSER.ORDER_ITEMS.md) via `SYS_C008786`
