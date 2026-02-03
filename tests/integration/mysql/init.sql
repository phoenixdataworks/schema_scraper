-- MySQL Test Schema for Schema Scraper
-- Contains 1-2 objects of each supported type

-- =============================================================================
-- TABLES
-- =============================================================================
CREATE TABLE customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL UNIQUE,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    phone VARCHAR(20),
    street VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(50),
    postal_code VARCHAR(20),
    country VARCHAR(50) DEFAULT 'USA',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    INDEX idx_customers_name (last_name, first_name),
    INDEX idx_customers_email (email)
) ENGINE=InnoDB COMMENT='Customer master table containing contact and shipping information';

CREATE TABLE products (
    product_id INT AUTO_INCREMENT PRIMARY KEY,
    sku VARCHAR(50) NOT NULL UNIQUE,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    unit_price DECIMAL(10,2) NOT NULL,
    quantity_in_stock INT NOT NULL DEFAULT 0,
    reorder_level INT DEFAULT 10,
    category VARCHAR(100),
    is_discontinued BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_products_category (category),
    INDEX idx_products_sku (sku),
    CONSTRAINT chk_products_unit_price CHECK (unit_price > 0),
    CONSTRAINT chk_products_quantity CHECK (quantity_in_stock >= 0)
) ENGINE=InnoDB COMMENT='Product catalog with inventory tracking';

CREATE TABLE orders (
    order_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled') DEFAULT 'pending',
    total_amount DECIMAL(12,2) NOT NULL DEFAULT 0,
    notes TEXT,
    INDEX idx_orders_customer (customer_id),
    INDEX idx_orders_date (order_date),
    INDEX idx_orders_status (status),
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT chk_total_amount CHECK (total_amount >= 0)
) ENGINE=InnoDB COMMENT='Customer orders with status tracking';

CREATE TABLE order_items (
    order_item_id INT AUTO_INCREMENT PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    UNIQUE KEY uk_order_product (order_id, product_id),
    CONSTRAINT fk_order_items_order FOREIGN KEY (order_id)
        REFERENCES orders(order_id) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT fk_order_items_product FOREIGN KEY (product_id)
        REFERENCES products(product_id) ON DELETE RESTRICT ON UPDATE CASCADE,
    CONSTRAINT chk_order_items_quantity CHECK (quantity > 0),
    CONSTRAINT chk_order_items_price CHECK (unit_price > 0),
    CONSTRAINT chk_order_items_discount CHECK (discount_percent >= 0 AND discount_percent <= 100)
) ENGINE=InnoDB COMMENT='Line items for each order';

-- =============================================================================
-- VIEWS
-- =============================================================================
CREATE VIEW customer_order_summary AS
SELECT
    c.customer_id,
    CONCAT(c.first_name, ' ', c.last_name) AS customer_name,
    c.email,
    COUNT(o.order_id) AS total_orders,
    COALESCE(SUM(o.total_amount), 0) AS lifetime_value,
    MAX(o.order_date) AS last_order_date
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email;

CREATE VIEW low_stock_products AS
SELECT
    product_id,
    sku,
    name,
    quantity_in_stock,
    reorder_level,
    reorder_level - quantity_in_stock AS units_to_order
FROM products
WHERE quantity_in_stock < reorder_level
  AND is_discontinued = FALSE;

-- =============================================================================
-- STORED PROCEDURES
-- =============================================================================
DELIMITER //

CREATE PROCEDURE create_order(
    IN p_customer_id INT,
    IN p_notes TEXT,
    OUT p_order_id INT
)
BEGIN
    INSERT INTO orders (customer_id, notes)
    VALUES (p_customer_id, p_notes);

    SET p_order_id = LAST_INSERT_ID();
END //

CREATE PROCEDURE update_stock(
    IN p_product_id INT,
    IN p_quantity_change INT
)
BEGIN
    DECLARE v_exists INT;

    SELECT COUNT(*) INTO v_exists FROM products WHERE product_id = p_product_id;

    IF v_exists = 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Product not found';
    END IF;

    UPDATE products
    SET quantity_in_stock = quantity_in_stock + p_quantity_change
    WHERE product_id = p_product_id;
END //

-- =============================================================================
-- FUNCTIONS
-- =============================================================================
CREATE FUNCTION calculate_order_total(p_order_id INT)
RETURNS DECIMAL(12,2)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_total DECIMAL(12,2);

    SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_percent/100)), 0)
    INTO v_total
    FROM order_items
    WHERE order_id = p_order_id;

    RETURN v_total;
END //

CREATE FUNCTION get_customer_name(p_customer_id INT)
RETURNS VARCHAR(100)
DETERMINISTIC
READS SQL DATA
BEGIN
    DECLARE v_name VARCHAR(100);

    SELECT CONCAT(first_name, ' ', last_name)
    INTO v_name
    FROM customers
    WHERE customer_id = p_customer_id;

    RETURN v_name;
END //

-- =============================================================================
-- TRIGGERS
-- =============================================================================
CREATE TRIGGER trg_order_items_after_insert
AFTER INSERT ON order_items
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = calculate_order_total(NEW.order_id)
    WHERE order_id = NEW.order_id;
END //

CREATE TRIGGER trg_order_items_after_update
AFTER UPDATE ON order_items
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = calculate_order_total(NEW.order_id)
    WHERE order_id = NEW.order_id;
END //

CREATE TRIGGER trg_order_items_after_delete
AFTER DELETE ON order_items
FOR EACH ROW
BEGIN
    UPDATE orders
    SET total_amount = calculate_order_total(OLD.order_id)
    WHERE order_id = OLD.order_id;
END //

DELIMITER ;

-- =============================================================================
-- SAMPLE DATA
-- =============================================================================
INSERT INTO customers (email, first_name, last_name, phone, city, state) VALUES
    ('john.doe@example.com', 'John', 'Doe', '555-0101', 'New York', 'NY'),
    ('jane.smith@example.com', 'Jane', 'Smith', '555-0102', 'Los Angeles', 'CA');

INSERT INTO products (sku, name, description, unit_price, quantity_in_stock, category) VALUES
    ('WIDGET-001', 'Standard Widget', 'A basic widget for everyday use', 19.99, 100, 'Widgets'),
    ('WIDGET-002', 'Premium Widget', 'A high-quality premium widget', 49.99, 50, 'Widgets'),
    ('GADGET-001', 'Mini Gadget', 'Compact and portable gadget', 29.99, 75, 'Gadgets');
