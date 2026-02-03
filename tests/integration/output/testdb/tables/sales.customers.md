# Sales.Customers

## Statistics

- **Rows:** 2
- **Total Space:** 72 KB

## Columns

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| CustomerID | int | NO | IDENTITY(1,1) |  |
| Email | nvarchar(255) | NO |  |  |
| FirstName | nvarchar(50) | NO |  |  |
| LastName | nvarchar(50) | NO |  |  |
| Phone | nvarchar(20) | YES |  |  |
| Street | nvarchar(200) | YES |  |  |
| City | nvarchar(100) | YES |  |  |
| State | nvarchar(50) | YES |  |  |
| PostalCode | nvarchar(20) | YES |  |  |
| Country | nvarchar(50) | YES | ('USA') |  |
| CreatedAt | datetime2 | YES | (getutcdate()) |  |
| UpdatedAt | datetime2 | YES | (getutcdate()) |  |
| IsActive | bit | YES | ((1)) |  |

## Primary Key

**PK__Customer__A4AE64B8E2FF602D** (CLUSTERED)

Columns: `CustomerID`

## Indexes

| Name | Type | Columns | Filter |
|------|------|---------|--------|
| UQ__Customer__A9D10534E3B68881 | UNIQUE NONCLUSTERED | Email |  |
| IX_Customers_Name | NONCLUSTERED | LastName, FirstName |  |
| IX_Customers_Email | NONCLUSTERED | Email |  |

## Relationships

### Referenced By (other tables → this table)

- ← [Sales.Orders](../Sales.Orders.md) via `FK__Orders__Customer__5535A963`
