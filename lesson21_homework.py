import psycopg2
import pandas as pd

import os
from dotenv import load_dotenv

load_dotenv()  # загружает переменные из .env

def get_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

# Задача 1. Экспорт расписания рейсов по конкретному маршруту.
# Создать функцию на Python, которая выгружает в CSV-файл расписание рейсов между двумя городами (например, Москва и Санкт-Петербург). Функция должна включать:
# - Номер рейса
# - Время вылета и прилета
# - Тип самолета
# - Среднюю цену билета

def export_flights_to_csv(file_path, dep_city, arr_city, conn):
    query = """
        with avg_tickets_price as (
	        select
		        tf.flight_id,
		        round(avg(tf.amount), 2) as avg_ticket_price
	        from bookings.ticket_flights tf
	        group by tf.flight_id
        )
        select
            f.flight_id,
            f.flight_no,
            f.scheduled_departure,
            f.scheduled_arrival,
            a.model,
            atp.avg_ticket_price
        from bookings.flights f
        join bookings.aircrafts a 
            on a.aircraft_code = f.aircraft_code
        join avg_tickets_price atp
            on atp.flight_id = f.flight_id
        where f.flight_id in (
            select f.flight_id from bookings.flights f
            join bookings.airports a_dep on a_dep.airport_code = f.departure_airport
            join bookings.airports a_arr on a_arr.airport_code = f.arrival_airport
            where a_dep.city = %s and a_arr.city = %s
        )
        order by f.flight_id;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(query, (dep_city, arr_city))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]  # имена колонок
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(file_path, index=False)
        print(f"Данные успешно экспортированы в {file_path}")
    conn.close()

# Задача 3. Динамическое ценообразование
# Реализовать функцию, которая автоматически корректирует цены на билеты в зависимости от спроса:
# - Повышает цены на 10%, если продано >80% мест
# - Понижает на 5%, если продано <30% мест
# - Не изменяет цены бизнес-класса

"""
Для реализации функции создано представление bookings.for_flight_occupacy_rate,
которое объединяет таблицы bookings.ticket_flights, bookings.flights,
bookings.seats, bookings.aircrafts и считает процент проданных мест (occ_rate)

create view bookings.for_flight_occupacy_rate as
select
	tf.flight_id,
	f.flight_no,
	count(*) as cnt_tickets,
	sc.aircraft_code,
	sc.seats_count,
	round(count(*) * 100 / sc.seats_count, 2) as occ_rate
from bookings.ticket_flights tf
join bookings.flights f 
	on f.flight_id = tf.flight_id
join (
	select
		a.aircraft_code,
		count(*) as seats_count
	from bookings.seats s
	join bookings.aircrafts a 
		on a.aircraft_code = s.aircraft_code
	group by a.aircraft_code
) as sc on sc.aircraft_code = f.aircraft_code
group by tf.flight_id, f.flight_no, sc.aircraft_code, sc.seats_count
order by tf.flight_id;
"""

def update_ticket_amount():
    conn = get_connection()
    cursor = conn.cursor()

    query = """
        update bookings.ticket_flights tf
        set amount = round((tf.amount * 1.1), 2)
        from bookings.for_flight_occupacy_rate ffor 
        where 
            ffor.flight_id = tf.flight_id 
            and ffor.occ_rate > 80
            and tf.fare_conditions != 'Business';
        update bookings.ticket_flights tf
        set amount = round((tf.amount * 0.95), 2)
        from bookings.for_flight_occupacy_rate ffor 
        where 
            ffor.flight_id = tf.flight_id 
            and ffor.occ_rate < 30
            and tf.fare_conditions != 'Business';
    """

    cursor.execute(query)
    conn.commit()
    print("Данные обновлены")
    conn.close()

if __name__ == '__main__':
 #   export_flights_to_csv('PythonProject/project/Homeworks/lesson21_files/Moscow_StPetersburg.csv', 'Москва', 'Санкт-Петербург')
    update_ticket_amount()