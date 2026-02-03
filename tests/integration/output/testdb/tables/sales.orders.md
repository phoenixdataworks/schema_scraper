# Sales.Orders

## Statistics

- **Rows:** 0
- **Total Space:** 0 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| OrderID | int | NO | (NEXT VALUE FOR [Sales].[OrderSeq]) |  |
| CustomerID | int | NO |  |  |
| OrderDate | datetime2 | YES | (getutcdate()) |  |
| Status | nvarchar(20) | YES | ('pending') |  |
| TotalAmount | decimal(12,2) | NO | ((0)) |  |
| Notes | nvarchar(max) | YES |  |  |

## Primary Key

**PK__Orders__C3905BAF74ED0AED** (CLUSTERED)

Columns: `OrderID`

## Foreign Keys

| Name | Columns | References | On Delete | On Update |
|------|---------|------------|-----------|-----------|
| FK__Orders__Customer__5535A963 | CustomerID | Sales.Customers(CustomerID) | NO ACTION | NO ACTION |

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| IX_Orders_Customer | NONCLUSTERED | CustomerID |  |
| IX_Orders_Date | NONCLUSTERED | OrderDate |  |
| IX_Orders_Status | NONCLUSTERED | Status |  |

## Check Constraints

### CHK_Orders_Status

```sql
([Status]='cancelled' OR [Status]='delivered' OR [Status]='shipped' OR [Status]='processing' OR [Status]='pending')
```

### CHK_Orders_TotalAmount

```sql
([TotalAmount]>=(0))
```

## Relationships

### References (this table → other tables)

- → [Sales.Customers](../Sales.Customers.md) via `FK__Orders__Customer__5535A963`

### Referenced By (other tables → this table)

- ← [Sales.OrderItems](../Sales.OrderItems.md) via `FK__OrderItem__Order__5DCAEF64`
