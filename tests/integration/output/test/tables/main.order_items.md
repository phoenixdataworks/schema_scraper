# main.order_items

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| order_item_id | INTEGER | YES | IDENTITY(1,1) |  |
| order_id | INTEGER | NO |  |  |
| product_id | INTEGER | NO |  |  |
| quantity | INTEGER | NO |  |  |
| unit_price | REAL | NO |  |  |
| discount_percent | REAL | YES | 0 |  |

## Primary Key

**pk_order_items** (CLUSTERED)

Columns: `order_item_id`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| fk_order_items_0 | product_id | main.products(product_id) | NO ACTION | NO ACTION |
| fk_order_items_1 | order_id | main.orders(order_id) | CASCADE | NO ACTION |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| sqlite_autoindex_order_items_1 | UNIQUE BTREE | order_id, product_id |  |

## Relationships

### References (this table → other tables)

- → [main.products](../main.products.md) via `fk_order_items_0`
- → [main.orders](../main.orders.md) via `fk_order_items_1`
