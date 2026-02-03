# testdb.order_items

Line items for each order

## Statistics

- **Rows:** 0
- **Total Space:** 48 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| order_item_id | int | NO | IDENTITY(1,1) |  |
| order_id | int | NO |  |  |
| product_id | int | NO |  |  |
| quantity | int | NO |  |  |
| unit_price | decimal(10,2) | NO |  |  |
| discount_percent | decimal(5,2) | YES | 0.00 |  |

## Primary Key

**PRIMARY** (CLUSTERED)

Columns: `order_item_id`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| fk_order_items_order | order_id | testdb.orders(order_id) | CASCADE | CASCADE |
| fk_order_items_product | product_id | testdb.products(product_id) | RESTRICT | CASCADE |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| fk_order_items_product | BTREE | product_id |  |
| uk_order_product | UNIQUE BTREE | order_id, product_id |  |

## Check Constraints

### chk_order_items_quantity

```sql
(`quantity` > 0)
```

### chk_order_items_price

```sql
(`unit_price` > 0)
```

### chk_order_items_discount

```sql
((`discount_percent` >= 0) and (`discount_percent` <= 100))
```

## Relationships

### References (this table → other tables)

- → [testdb.orders](../testdb.orders.md) via `fk_order_items_order`
- → [testdb.products](../testdb.products.md) via `fk_order_items_product`
