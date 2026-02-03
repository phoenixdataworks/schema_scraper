# Triggers

Total: 4 triggers

| Schema | Trigger | Table | Type | Events | Disabled |
|--------|---------|-------|------|--------|----------|
| main | [trg_customers_update_timestamp](main.trg_customers_update_timestamp.md) | customers | AFTER | UPDATE | No |
| main | [trg_order_items_delete_update_total](main.trg_order_items_delete_update_total.md) | order_items | AFTER | UPDATE, DELETE | No |
| main | [trg_order_items_insert_update_total](main.trg_order_items_insert_update_total.md) | order_items | AFTER | INSERT, UPDATE | No |
| main | [trg_order_items_update_update_total](main.trg_order_items_update_update_total.md) | order_items | AFTER | UPDATE | No |