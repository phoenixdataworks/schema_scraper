# Inventory.LowStockProducts

## Columns

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| ProductID | int | NO |  |
| SKU | nvarchar(50) | NO |  |
| Name | nvarchar(200) | NO |  |
| QuantityInStock | int | NO |  |
| ReorderLevel | int | YES |  |
| UnitsToOrder | int | YES |  |

## Base Tables

- Inventory.Products

## Definition

```sql

CREATE VIEW Inventory.LowStockProducts AS
SELECT ProductID, SKU, Name, QuantityInStock, ReorderLevel, ReorderLevel - QuantityInStock AS UnitsToOrder
FROM Inventory.Products WHERE QuantityInStock < ReorderLevel AND IsDiscontinued = 0;

```
