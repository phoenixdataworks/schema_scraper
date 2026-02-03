# TESTUSER.GET_CUSTOMER_NAME

**Type:** Scalar
**Language:** PL/SQL

## Parameters

| Name | Type | Default |
|------|------|---------|
| P_CUSTOMER_ID | NUMBER |  |

## Returns

`VARCHAR2`

## Definition

```sql
FUNCTION get_customer_name(p_customer_id IN NUMBER)
RETURN VARCHAR2
IS
    v_name VARCHAR2(100);
BEGIN
    SELECT first_name || ' ' || last_name
    INTO v_name
    FROM customers
    WHERE customer_id = p_customer_id;

    RETURN v_name;
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        RETURN NULL;
END get_customer_name;
```
