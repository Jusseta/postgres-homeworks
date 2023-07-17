"""Скрипт для заполнения данными таблиц в БД Postgres."""


import psycopg2
import csv


employees_data = './north_data/employees_data.csv'
customers_data = './north_data/customers_data.csv'
orders_data = './north_data/orders_data.csv'


connection = psycopg2.connect(
    host='localhost',
    database='north',
    user='postgres',
    password='qwerty'
)

try:
    with connection:
        with connection.cursor() as cursor:
            with open(employees_data) as file:
                reader = csv.DictReader(file)
                for i in reader:
                    first_name = i['first_name']
                    last_name = i['last_name']
                    title = i['title']
                    birth_date = i['birth_date']
                    notes = i['notes']

                    cursor.executemany('INSERT INTO employees VALUES (default, %s, %s, %s, %s, %s)',
                                       [(first_name, last_name, title, birth_date, notes)])

            with open(customers_data) as file:
                reader = csv.DictReader(file)
                for i in reader:
                    customer_id = i['customer_id']
                    company_name = i['company_name']
                    contact_name = i['contact_name']

                    cursor.executemany('INSERT INTO customers VALUES (%s, %s, %s)',
                                       [(customer_id, company_name, contact_name)])

            with open(orders_data) as file:
                reader = csv.DictReader(file)
                for i in reader:
                    order_id = i['order_id']
                    customer_id = i['customer_id']
                    employee_id = i['employee_id']
                    order_date = i['order_date']
                    ship_city = i['ship_city']

                    cursor.executemany('INSERT INTO orders VALUES (%s, %s, %s, %s, %s)',
                                       [(order_id, customer_id, employee_id, order_date, ship_city)])

finally:
    connection.close()
