INSERT INTO products (name, sku, description) VALUES
('Laptop HP', 'LAP-HP-001', 'Laptop HP 15 pulgadas'),
('Mouse Logitech', 'MOU-LOG-001', 'Mouse inalámbrico Logitech'),
('Teclado Mecánico', 'KEY-MEC-001', 'Teclado mecánico RGB'),
('Monitor Samsung', 'MON-SAM-001', 'Monitor 27 pulgadas 4K'),
('Auriculares Sony', 'AUD-SON-001', 'Auriculares bluetooth Sony'),
('Webcam Logitech', 'WEC-LOG-001', 'Webcam HD Logitech'),
('Disco SSD Samsung', 'SSD-SAM-001', 'SSD 1TB NVMe'),
('Memoria RAM Kingston', 'RAM-KIN-001', 'RAM 16GB DDR4'),
('Cable HDMI', 'CAB-HDM-001', 'Cable HDMI 2.0 2m'),
('Mousepad XL', 'MPA-XL-001', 'Mousepad extra large');

INSERT INTO stock (product_id, quantity_available, quantity_reserved) VALUES
(1, 50, 0),
(2, 200, 0),
(3, 150, 0),
(4, 30, 0),
(5, 100, 0),
(6, 80, 0),
(7, 120, 0),
(8, 200, 0),
(9, 500, 0),
(10, 300, 0);
