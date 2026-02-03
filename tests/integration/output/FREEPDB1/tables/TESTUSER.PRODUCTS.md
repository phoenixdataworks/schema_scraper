# TESTUSER.PRODUCTS

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| PRODUCT_ID | NUMBER | NO | "TESTUSER"."PRODUCT_SEQ"."NEXTVAL" |  |
| SKU | VARCHAR2 | NO |  |  |
| NAME | VARCHAR2 | NO |  |  |
| DESCRIPTION | CLOB | YES |  |  |
| UNIT_PRICE | NUMBER | NO |  |  |
| QUANTITY_IN_STOCK | NUMBER | NO | 0 |  |
| REORDER_LEVEL | NUMBER | YES | 10 |  |
| CATEGORY | VARCHAR2 | YES |  |  |
| IS_DISCONTINUED | NUMBER | YES | 0 |  |
| CREATED_AT | TIMESTAMP(6) | YES | CURRENT_TIMESTAMP |  |

## Primary Key

**SYS_C008768** (NONCLUSTERED)

Columns: `PRODUCT_ID`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| IDX_PRODUCTS_CATEGORY | NORMAL | CATEGORY |  |
| SYS_C008769 | UNIQUE NORMAL | SKU |  |

## Relationships

### Referenced By (other tables → this table)

- ← [TESTUSER.ORDER_ITEMS](../TESTUSER.ORDER_ITEMS.md) via `SYS_C008787`
