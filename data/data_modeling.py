import pandas as pd
import numpy as np
import os
import random
import sqlite3
from faker import Faker
from datetime import datetime, timedelta

# Conexión a la base
conn = sqlite3.connect("medical_star.db")
cur = conn.cursor()

# Create tables
cur.executescript(
'''
-- 🔄 Eliminación de tablas si existen (en orden para evitar conflictos de FK)
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS employees;
DROP TABLE IF EXISTS shippers;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS suppliers;
DROP TABLE IF EXISTS dates;

-- 📅 DimDate
CREATE TABLE dates (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    week INTEGER,
    day_of_week TEXT
);

-- 👥 DimCustomer
CREATE TABLE customers (
    customer_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT,              
    city TEXT,
    region TEXT,
    country TEXT
);

-- 📦 DimProduct
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT,          
    manufacturer TEXT,
    unit_of_measure TEXT,   
    regulatory_code TEXT,   
    supplier_id INTEGER,
    FOREIGN KEY (supplier_id) REFERENCES suppliers(supplier_id)
);

-- 🚚 DimSupplier
CREATE TABLE suppliers (
    supplier_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    country TEXT
);

-- 👨‍💼 DimEmployee
CREATE TABLE employees (
    employee_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    role TEXT,              
    hire_date TIMESTAMP
);

-- 🚛 DimShipper
CREATE TABLE shippers (
    shipper_id INTEGER PRIMARY KEY,
    company_name TEXT NOT NULL,
    region_covered TEXT
);

-- ⭐ FactOrders
CREATE TABLE orders (
    order_id INTEGER PRIMARY KEY,
    date_id INTEGER NOT NULL,
    customer_id INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    employee_id INTEGER NOT NULL,
    shipper_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    unit_price REAL NOT NULL,
    discount REAL DEFAULT 0,
    total_amount REAL GENERATED ALWAYS AS (quantity * unit_price * (1 - discount)) STORED,
    FOREIGN KEY (date_id) REFERENCES dates(date_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
    FOREIGN KEY (shipper_id) REFERENCES shippers(shipper_id)
);
'''
)

fake = Faker()
Faker.seed(42)
random.seed(42)

# ----------------------------
# Insert suppliers
# ----------------------------
suppliers = []
for i in range(20):
    name = fake.company()
    country = fake.country()
    cur.execute("INSERT INTO suppliers (name, country) VALUES (?, ?)", (name, country))
    suppliers.append(cur.lastrowid)

# ----------------------------
# Insert products
# ----------------------------
categories = ["Surgical", "Diagnostic", "Consumables", "Equipment", "Pharmaceutical", "Protective Gear", "Laboratory"]
for i in range(30):
    cur.execute(
        """INSERT INTO products (product_name, category, manufacturer, unit_of_measure, regulatory_code, supplier_id)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            fake.word().capitalize(),
            random.choice(categories),
            fake.company(),
            random.choice(["piece", "box", "batch"]),
            random.choice(["CE", "FDA"]),
            random.choice(suppliers),
        ),
    )

# ----------------------------
# Insert customers
# ----------------------------
types = ["Hospital", "Clinic", "Pharmacy", "Distributor", "Individual"]
for i in range(20):
    cur.execute(
        """INSERT INTO customers (name, type, city, region, country)
           VALUES (?, ?, ?, ?, ?)""",
        (
            fake.company(),
            random.choice(types),
            fake.city(),
            fake.state(),
            fake.country(),
        ),
    )

# ----------------------------
# Insert employees
# ----------------------------
roles = ["Sales", "Support", "Logistics"]
for i in range(15):
    cur.execute(
        """INSERT INTO employees (name, role, hire_date)
           VALUES (?, ?, ?)""",
        (
            fake.name(),
            random.choice(roles),
            fake.date_between(start_date="-5y", end_date="today"),
        ),
    )

# ----------------------------
# Insert shippers
# ----------------------------
for i in range(20):
    cur.execute(
        """INSERT INTO shippers (company_name, region_covered)
           VALUES (?, ?)""",
        (
            fake.company(),
            random.choice(["Europe", "North America", "Global", "Local"]),
        ),
    )

# ----------------------------
# Insert dates (DimDate)
# ----------------------------
date_ids = {}
start_date = datetime.now() - timedelta(days=5*365)  # Últimos 5 años
for i in range(5*365):
    d = start_date + timedelta(days=i)
    cur.execute(
        """INSERT INTO dates (full_date, year, quarter, month, week, day_of_week)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (
            d.date(),
            d.year,
            (d.month - 1) // 3 + 1,
            d.month,
            d.isocalendar()[1],
            d.strftime("%A"),
        ),
    )
    date_ids[d.date()] = cur.lastrowid

# ----------------------------
# Insert orders (FactOrders)
# ----------------------------
customer_ids = [row[0] for row in cur.execute("SELECT customer_id FROM customers")]
product_ids = [row[0] for row in cur.execute("SELECT product_id FROM products")]
employee_ids = [row[0] for row in cur.execute("SELECT employee_id FROM employees")]
shipper_ids = [row[0] for row in cur.execute("SELECT shipper_id FROM shippers")]

for i in range(2000):
    # Elegir un date_id válido
    date_id = random.choice(list(date_ids.values()))
    customer_id = random.choice(customer_ids)
    product_id = random.choice(product_ids)
    employee_id = random.choice(employee_ids)
    shipper_id = random.choice(shipper_ids)

    quantity = random.randint(1, 10)
    unit_price = round(random.uniform(10, 1000), 2)
    discount = round(random.choice([0, 0.05, 0.1, 0.15]), 2)

    cur.execute(
        """INSERT INTO orders (date_id, customer_id, product_id, employee_id, shipper_id, 
                               quantity, unit_price, discount) 
           VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
        (date_id, customer_id, product_id, employee_id, shipper_id,
         quantity, unit_price, discount),
    )

# Guardar cambios
conn.commit()
conn.close()
