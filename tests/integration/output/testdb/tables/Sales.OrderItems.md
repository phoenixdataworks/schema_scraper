# Sales.OrderItems

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| OrderItemID | int | NO | IDENTITY(1,1) |  |
| OrderID | int | NO |  |  |
| ProductID | int | NO |  |  |
| Quantity | int | NO |  |  |
| UnitPrice | decimal(10,2) | NO |  |  |
| DiscountPercent | decimal(5,2) | YES | ((0)) |  |

## Primary Key

**PK__OrderIte__57ED06A1A59A67A8** (CLUSTERED)

Columns: `OrderItemID`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| FK__OrderItem__Produ__5EBF139D | ProductID | Inventory.Products(ProductID) | NO ACTION | NO ACTION |
| FK__OrderItem__Order__5DCAEF64 | OrderID | Sales.Orders(OrderID) | CASCADE | NO ACTION |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| UQ_OrderItems_OrderProduct | UNIQUE NONCLUSTERED | OrderID, ProductID |  |

## Check Constraints

### CHK_OrderItems_Quantity

```sql
([Quantity]>(0))
```

### CHK_OrderItems_UnitPrice

```sql
([UnitPrice]>(0))
```

### CHK_OrderItems_Discount

```sql
([DiscountPercent]>=(0) AND [DiscountPercent]<=(100))
```

## Relationships

### References (this table → other tables)

- → [Inventory.Products](../Inventory.Products.md) via `FK__OrderItem__Produ__5EBF139D`
- → [Sales.Orders](../Sales.Orders.md) via `FK__OrderItem__Order__5DCAEF64`
