# testdb.low_stock_products

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| product_id | int | NO |  |
| sku | varchar(50) | NO |  |
| name | varchar(200) | NO |  |
| quantity_in_stock | int | NO |  |
| reorder_level | int | YES |  |
| units_to_order | bigint | YES |  |

## Definition

```sql
select `testdb`.`products`.`product_id` AS `product_id`,`testdb`.`products`.`sku` AS `sku`,`testdb`.`products`.`name` AS `name`,`testdb`.`products`.`quantity_in_stock` AS `quantity_in_stock`,`testdb`.`products`.`reorder_level` AS `reorder_level`,(`testdb`.`products`.`reorder_level` - `testdb`.`products`.`quantity_in_stock`) AS `units_to_order` from `testdb`.`products` where ((`testdb`.`products`.`quantity_in_stock` < `testdb`.`products`.`reorder_level`) and (`testdb`.`products`.`is_discontinued` = false))
```
