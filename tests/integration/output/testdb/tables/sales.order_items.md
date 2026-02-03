# sales.order_items

## Statistics

- **Rows:** -1
- **Total Space:** 16 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| order_item_id | integer | NO | nextval('sales.order_items_order_item_id_seq'::regclass) |  |
| order_id | integer | NO |  |  |
| product_id | integer | NO |  |  |
| quantity | integer | NO |  |  |
| unit_price | numeric(10,2) | NO |  |  |
| discount_percent | numeric(5,2) | YES | 0 |  |

## Primary Key

**order_items_pkey** (NONCLUSTERED)

Columns: `{`, `o`, `r`, `d`, `e`, `r`, `_`, `i`, `t`, `e`, `m`, `_`, `i`, `d`, `}`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| order_items_order_id_fkey | {, o, r, d, e, r, _, i, d, } | sales.orders({, o, r, d, e, r, _, i, d, }) | CASCADE | NO ACTION |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| order_items_order_id_product_id_key | UNIQUE BTREE | order_id, product_id |  |

## Check Constraints

### order_items_unit_price_check

```sql
((unit_price > (0)::numeric))
```

### order_items_discount_percent_check

```sql
(((discount_percent >= (0)::numeric) AND (discount_percent <= (100)::numeric)))
```

### order_items_quantity_check

```sql
((quantity > 0))
```

## Relationships

### References (this table → other tables)

- → [sales.orders](../sales.orders.md) via `order_items_order_id_fkey`
