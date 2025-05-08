-- Задача 1: Анализ частоты полетов пассажиров
-- Определить топ-10 пассажиров, которые чаще всего летают
-- Провести оптимизацию скрипта по необходимости

explain analyze
select
	t.passenger_name,
	count(tf.flight_id) as flights_cnt
from bookings.ticket_flights tf
join bookings.tickets t
	on tf.ticket_no = t.ticket_no
group by t.passenger_name
order by flights_cnt desc 
limit 10;

-- Execution Time: 280.736 ms
-- объем таблицы ticket_flights - 168M

-- создание индексов
create index idx_ticket_flights_ticket_no on bookings.ticket_flights(ticket_no); -- так как используется в join

-- Execution Time: 261.548 ms
-- объем таблицы ticket_flights - 186M
-- время выполнения уменьшилось на 6,8%, объем таблицы вырос на 9,7%

-- Задача 2. Анализ загрузки самолетов по дням недели
-- Определить, в какие дни недели самолеты загружены больше всего

-- Логика расчета. Шаги:
-- - Используем данные о занятых местах (boarding_passes) и общем количестве мест (seats).
-- - Группируем данные по дням недели.
-- - Рассчитаем среднюю загрузку самолетов для каждого дня.

-- создание представления bookings.dow_flight_occupacy_rate,
-- которое объединяет таблицы bookings.ticket_flights, bookings.flights,
-- bookings.seats, bookings.aircrafts и считает процент проданных мест/загрузку самолета (occ_rate)
-- с указанием дня недели (day_of_week), когда совершался рейс
create view bookings.dow_flight_occupacy_rate as
select
	tf.flight_id,
	f.flight_no,
	to_char(f.scheduled_departure, 'Day') as day_of_week,
	count(*) as cnt_sold_tickets,
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
group by tf.flight_id, f.flight_no, f.scheduled_departure, sc.aircraft_code, sc.seats_count;

select * from bookings.dow_flight_occupacy_rate;

select 
	day_of_week,
	round(avg(occ_rate), 2) as avg_occ_rate
from bookings.dow_flight_occupacy_rate
group by day_of_week
order by avg_occ_rate desc;


-- GroupAggregate — это способ выполнения GROUP BY, при котором
-- сначала происходит сортировка данных по полям группировки, затем их подсчет 
-- - не требует создания хэш-таблицы
-- - данные отсортированны (есть индекс)

-- создание индекса
create index idx_flights_status on bookings.flights(status);

explain analyze
select
	status,
	count(*) as cnt
from bookings.flights
group by status;

-- GroupAggregate  (cost=0.29..786.77 rows=6 width=16) (actual time=3.702..7.205 rows=6 loops=1)
--   Group Key: status
--   ->  Index Only Scan using idx_flights_status on flights  (cost=0.29..621.11 rows=33121 width=8) (actual time=0.049..3.701 rows=33121 loops=1)
--         Heap Fetches: 0