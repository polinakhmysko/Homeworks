create database tech_gadgets;

create schema production;
create schema analytics;
create schema sandbox;

-- Таблицы в схеме production
CREATE TABLE production.products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL,
    category VARCHAR(50),
    stock_quantity INT NOT NULL
);

CREATE TABLE production.customers (
    customer_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    registration_date DATE NOT NULL
);

CREATE TABLE production.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES production.customers(customer_id),
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(20) NOT NULL
);

CREATE TABLE production.order_items (
    item_id SERIAL PRIMARY KEY,
    order_id INT REFERENCES production.orders(order_id),
    product_id INT REFERENCES production.products(product_id),
    quantity INT NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

-- Таблицы в схеме analytics
CREATE TABLE analytics.sales_stats (
    stat_id SERIAL PRIMARY KEY,
    period DATE NOT NULL,
    total_sales DECIMAL(12,2) NOT NULL,
    top_product_id INT REFERENCES production.products(product_id)
);

CREATE TABLE analytics.customer_segments (
    segment_id SERIAL PRIMARY KEY,
    segment_name VARCHAR(50) NOT NULL,
    criteria TEXT NOT NULL,
    customer_count INT NOT NULL
);

create role data_engineer with password 'eng123';
GRANT CONNECT ON DATABASE tech_gadgets TO data_engineer;
GRANT USAGE ON SCHEMA production, analytics, sandbox TO data_engineer;
grant all privileges on all tables in schema production, analytics, sandbox to data_engineer;

create role analyst with password 'an123';
GRANT CONNECT ON DATABASE tech_gadgets TO analyst;
GRANT USAGE ON SCHEMA production, analytics, sandbox TO analyst;
grant select on all tables in schema production, analytics to analyst;
grant all privileges on all tables in schema sandbox to analyst;

create role manager with password 'manag123';
GRANT CONNECT ON DATABASE tech_gadgets TO manager;
GRANT USAGE ON SCHEMA analytics TO manager;
grant select on all tables in schema analytics to manager;

-- посмотреть все права к таблицам по пользователям:
SELECT 
    table_schema,
    table_name,
    privilege_type, 
    grantee
FROM information_schema.table_privileges;

-- создание представления с продажами по категориям
create view analytics.sales_in_categories as
select
	p.category,
	sum(oi.quantity) as total_quantity,
	sum(oi.quantity * oi.price) as total_sales
from production.order_items oi
join production.products p
	on p.product_id = oi.product_id
group by p.category;

-- доступ менеджерам только к представлению в схеме analytics
revoke all privileges on all tables in schema analytics from manager;
grant all privileges on table analytics.sales_in_categories to manager;

-- создание роли senior_analyst с правами аналитиков и возможностью создавать временные таблицы
create role senior_analyst with password 'sen_an123';
GRANT CONNECT ON DATABASE tech_gadgets TO senior_analyst;
GRANT USAGE ON SCHEMA production, analytics, sandbox TO senior_analyst;
grant analyst to senior_analyst;

-- по умолчанию все пользователи,которые могут подключаться к базе, автоматически имеют право создавать временные таблицы
-- отбираем это право у всех ролей
REVOKE TEMP ON DATABASE tech_gadgets FROM PUBLIC;

-- предоставляем право только роли senior_analyst
grant TEMP on database tech_gadgets to senior_analyst;

-- проверка, у кого есть право создавать временные таблицы
select has_database_privilege('senior_analyst', 'tech_gadgets', 'TEMP');