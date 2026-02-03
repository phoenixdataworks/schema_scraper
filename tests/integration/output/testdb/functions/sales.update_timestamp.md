# sales.update_timestamp

**Type:** Scalar
**Language:** plpgsql

## Returns

`trigger`

## Definition

```sql
CREATE OR REPLACE FUNCTION sales.update_timestamp()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$function$

```
