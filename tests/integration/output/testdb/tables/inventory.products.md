# Inventory.Products

## Statistics

- **Rows:** 9
- **Total Space:** 72 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| ProductID | int | NO | (NEXT VALUE FOR [Inventory].[ProductSeq]) |  |
| SKU | nvarchar(50) | NO |  |  |
| Name | nvarchar(200) | NO |  |  |
| Description | nvarchar(max) | YES |  |  |
| UnitPrice | decimal(10,2) | NO |  |  |
| QuantityInStock | int | NO | ((0)) |  |
| ReorderLevel | int | YES | ((10)) |  |
| Category | nvarchar(100) | YES |  |  |
| IsDiscontinued | bit | YES | ((0)) |  |
| CreatedAt | datetime2 | YES | (getutcdate()) |  |

## Primary Key

**PK__Products__B40CC6ED3685A75D** (CLUSTERED)

Columns: `ProductID`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| UQ__Products__CA1ECF0D7BAAE2BF | UNIQUE NONCLUSTERED | SKU |  |
| IX_Products_Category | NONCLUSTERED | Category |  |
| IX_Products_SKU | NONCLUSTERED | SKU |  |

## Check Constraints

### CHK_Products_UnitPrice

```sql
([UnitPrice]>(0))
```

### CHK_Products_Quantity

```sql
([QuantityInStock]>=(0))
```

## Relationships

### Referenced By (other tables → this table)

- ← [Sales.OrderItems](../Sales.OrderItems.md) via `FK__OrderItem__Produ__5EBF139D`
