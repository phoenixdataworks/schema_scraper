# TESTUSER.ORDER_ITEMS

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| ORDER_ITEM_ID | NUMBER | NO | IDENTITY(1,1) |  |
| ORDER_ID | NUMBER | NO |  |  |
| PRODUCT_ID | NUMBER | NO |  |  |
| QUANTITY | NUMBER | NO |  |  |
| UNIT_PRICE | NUMBER | NO |  |  |
| DISCOUNT_PERCENT | NUMBER | YES | 0 |  |

## Primary Key

**SYS_C008784** (NONCLUSTERED)

Columns: `ORDER_ITEM_ID`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| SYS_C008787 | PRODUCT_ID | TESTUSER.PRODUCTS(PRODUCT_ID) | NO ACTION | NO ACTION |
| SYS_C008786 | ORDER_ID | TESTUSER.ORDERS(ORDER_ID) | CASCADE | NO ACTION |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| UK_ORDER_PRODUCT | UNIQUE NORMAL | ORDER_ID, PRODUCT_ID |  |

## Relationships

### References (this table → other tables)

- → [TESTUSER.PRODUCTS](../TESTUSER.PRODUCTS.md) via `SYS_C008787`
- → [TESTUSER.ORDERS](../TESTUSER.ORDERS.md) via `SYS_C008786`
