-- Задача 1: Анализ распределения мест в самолетах по классам обслуживания. Рассчитать:
-- - Общее количество мест в каждом самолете
-- - Количество мест каждого класса
-- - Процентное соотношение классов
-- - Массив всех мест для каждого самолета

select * from bookings.seats;

select
	aircraft_code,
	count(*) as seats_amount,
	count(*) filter (where fare_conditions = 'Business') as business_stats,
	count(*) filter (where fare_conditions = 'Comfort') as comfort_stats,
	count(*) filter (where fare_conditions = 'Economy') as economy_stats,
	round((count(*) filter (where fare_conditions = 'Business') * 100.0 / count(*)), 2) as business_percentage,
	round((count(*) filter (where fare_conditions = 'Comfort') * 100.0 / count(*)), 2) as comfort_percentage,
	round((count(*) filter (where fare_conditions = 'Economy') * 100.0 / count(*)), 2) as economy_percentage,
	(array_agg(seat_no order by seat_no)) as seats_array
from bookings.seats
group by aircraft_code;

-- Задача 2: Анализ стоимости билетов по рейсам
-- Для каждого рейса рассчитать:
-- - Минимальную, максимальную и среднюю стоимость билета
-- - Разницу между самым дорогим и самым дешевым билетом
-- - Медианную стоимость билета
-- - Массив всех цен на билеты

select * from bookings.ticket_flights;

select
	flight_id,
	min(amount) as min_ticket_amount,
	max(amount) as max_ticket_amount,
	round((avg(amount)), 2) as avg_ticket_amount,
	max(amount)- min(amount) as distinction_max_min,
	PERCENTILE_CONT(0.5) WITHIN GROUP (order by amount) as median_ticket_amount,
	(array_agg(amount order by amount)) as array_ticket_amount
from bookings.ticket_flights
group by flight_id;

-- Задача 3: Статистика по бронированиям по месяцам
-- Проанализировать бронирования по месяцам:
-- - Количество бронирований
-- - Общую сумму бронирований
-- - Средний чек
-- - Массив всех сумм бронирований для анализа распределения

select * from bookings.bookings;

select
	extract(month from book_date) as month_number,
	count(*) as month_bookings_count,
	sum(total_amount) as month_total_amount,
	round((avg(total_amount)), 2) as month_avg_amount,
	(array_agg(total_amount order by total_amount)) as array_total_amount
from bookings.bookings
group by month_number
order by month_number;

-- Задача 4: Анализ пассажиропотока по аэропортам
-- Рассчитать для каждого аэропорта:
-- - Общее количество вылетов
-- - Количество уникальных аэропортов назначения
-- - Массив всех аэропортов назначения

select * from bookings.flights;

select
	departure_airport,
	count(*) as flights_count,
	count(distinct arrival_airport) as arrival_airports_count,
	(array_agg(arrival_airport order by arrival_airport)) as array_arrival_airport
from bookings.flights
group by departure_airport;


-- Доп.задание
-- HAVING - фильтрует агрегированные данные (после GROUP BY)
select * from bookings.seats;

select
	aircraft_code,
	count(*) filter (where fare_conditions = 'Business') as business_stats
from bookings.seats
group by aircraft_code
having count(*) filter (where fare_conditions = 'Business') > 25;

-- EPOCH - вычисляет разницу между двумя временными метками в СЕКУНДАХ
select * from bookings.flights;

select
	flight_id,
	scheduled_arrival,
	scheduled_departure,
	AGE(scheduled_arrival,scheduled_departure) as flight_duration,
	EXTRACT(EPOCH from scheduled_arrival - scheduled_departure) as flight_duration_seconds
from bookings.flights;

-- PERCENTILE_CONT - можно использовать для подсчета медианы
select * from bookings.ticket_flights;

select
	flight_id,
	PERCENTILE_CONT(0.5) WITHIN GROUP (order by amount) as median_ticket_amount,
from bookings.ticket_flights
group by flight_id;

-- DATE_TRUNC() - округляет дату до начала месяца/года/дня
-- ROW_NUMBER() — нумерует строки внутри группы в определённом порядке
-- 			можно использовать для получения топ значений в группе
select * from bookings.bookings;

select *
from (
	select
		DATE_TRUNC('month', book_date) as month_start,
		sum(total_amount) as bookings_total_count,
		ROW_NUMBER() OVER (ORDER BY sum(total_amount) DESC) AS rank
	from bookings.bookings
	group by month_start
	order by month_start
)
where rank = 1;

-- ROW_NUMBER() + PARTITION BY - можно использовать для получения топ значений в каждой группе
select * from bookings.ticket_flights;

select *
from (
	select
		flight_id,
		fare_conditions,
		sum(amount) as flight_total_amount, -- по каждому классу
		ROW_NUMBER() OVER (PARTITION by flight_id ORDER BY sum(amount) DESC) AS rank
	from bookings.ticket_flights
	group by flight_id, fare_conditions
)
where rank = 1;