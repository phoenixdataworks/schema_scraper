# TESTUSER.CUSTOMERS

Customer master table containing contact and shipping information

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| CUSTOMER_ID | NUMBER | NO | IDENTITY(1,1) | Unique customer identifier |
| EMAIL | VARCHAR2 | NO |  | Customer email address (unique) |
| FIRST_NAME | VARCHAR2 | NO |  |  |
| LAST_NAME | VARCHAR2 | NO |  |  |
| PHONE | VARCHAR2 | YES |  |  |
| SHIPPING_ADDRESS | ADDRESS_TYPE | YES |  |  |
| CREATED_AT | TIMESTAMP(6) | YES | CURRENT_TIMESTAMP |  |
| UPDATED_AT | TIMESTAMP(6) | YES | CURRENT_TIMESTAMP |  |
| IS_ACTIVE | NUMBER | YES | 1 |  |

## Primary Key

**SYS_C008759** (NONCLUSTERED)

Columns: `CUSTOMER_ID`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| IDX_CUSTOMERS_NAME | NORMAL | LAST_NAME, FIRST_NAME |  |
| SYS_C008760 | UNIQUE NORMAL | EMAIL |  |

## Relationships

### Referenced By (other tables → this table)

- ← [TESTUSER.ORDERS](../TESTUSER.ORDERS.md) via `SYS_C008775`
