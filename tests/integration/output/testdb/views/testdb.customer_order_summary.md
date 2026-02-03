# testdb.customer_order_summary

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| customer_id | int | NO |  |
| customer_name | varchar(101) | YES |  |
| email | varchar(255) | NO |  |
| total_orders | bigint | NO |  |
| lifetime_value | decimal(34,2) | NO |  |
| last_order_date | timestamp | YES |  |

## Definition

```sql
select `c`.`customer_id` AS `customer_id`,concat(`c`.`first_name`,' ',`c`.`last_name`) AS `customer_name`,`c`.`email` AS `email`,count(`o`.`order_id`) AS `total_orders`,coalesce(sum(`o`.`total_amount`),0) AS `lifetime_value`,max(`o`.`order_date`) AS `last_order_date` from (`testdb`.`customers` `c` left join `testdb`.`orders` `o` on((`c`.`customer_id` = `o`.`customer_id`))) group by `c`.`customer_id`,`c`.`first_name`,`c`.`last_name`,`c`.`email`
```
