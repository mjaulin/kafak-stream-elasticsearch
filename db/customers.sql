DROP TABLE IF EXISTS customers;

CREATE TABLE customers
(
    id        integer PRIMARY KEY,
    firstName text NOT NULL,
    lastName  text NOT NULL,
    email     text NOT NULL,
    address   text NOT NULL,
    level     text NOT NULL
);

INSERT INTO customers VALUES (1, 'Jay', 'Kreps', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'platinum');
INSERT INTO customers VALUES (2, 'Neha', 'Narkhede', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'platinum');
INSERT INTO customers VALUES (3, 'Jun', 'Rao', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'platinum');
INSERT INTO customers VALUES (4, 'Trisha', 'Smith', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'bronze');
INSERT INTO customers VALUES (5, 'Monica', 'Brown', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'gold');
INSERT INTO customers VALUES (6, 'Gaurav', 'Night', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'silver');
INSERT INTO customers VALUES (7, 'Amanda', 'Leeworth', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'gold');
INSERT INTO customers VALUES (8, 'Lisa', 'Champion', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'silver');
INSERT INTO customers VALUES (9, 'Bob', 'West', 'devnull@example.com', '10 rue Victor Hugo, 44000 Nantes, France', 'bronze');
