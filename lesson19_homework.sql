-- Задача 1: 
-- Проанализировать данные о продажах билетов, чтобы получить статистику в следующих разрезах:
-- - По классам обслуживания (fare_conditions)
-- - По месяцам вылета
-- - По аэропортам вылета
-- - По комбинациям: класс + месяц, класс + аэропорт, месяц + аэропорт
-- - Общие итоги

select
	tf.fare_conditions,
	a.airport_name,
	extract(month from scheduled_departure) as month,
	count(*) as tickets_count,
	sum(tf.amount) as tickets_total_amount,
	round(avg(tf.amount), 2) as avg_ticket_amount
from bookings.flights f
join bookings.ticket_flights tf
	on tf.flight_id = f.flight_id
join bookings.airports a
	on a.airport_code = f.departure_airport
group by grouping sets (
	(tf.fare_conditions),
	(month),
	(a.airport_name),
	(tf.fare_conditions, month),
	(tf.fare_conditions, a.airport_name),
	(month, a.airport_name),
	(),
	(tf.fare_conditions, month, a.airport_name)
)
order by a.airport_name;

-- Задача 2. Рейсы с задержкой больше средней (CTE + подзапрос)

with
	dep_delay as (
		select
			round(avg(EXTRACT(EPOCH from actual_departure - scheduled_departure)), 2) as avg_departure_delay
		from bookings.flights f
		where actual_departure is not null
	)
select
	f.flight_no,
	EXTRACT(EPOCH from f.actual_departure - f.scheduled_departure) as departure_delay,
	d.avg_departure_delay
from bookings.flights f
cross join dep_delay d
where EXTRACT(EPOCH from f.actual_departure - f.scheduled_departure) > d.avg_departure_delay
;

-- Задача 3:
-- Создайте представление, которое содержит все рейсы, вылетающие из Москвы
create view bookings.flights_from_Moscow as
select
	*
from bookings.flights f
join bookings.airports a
	on a.airport_code = f.departure_airport
where a.city = 'Москва';


-- Временные таблицы — это таблицы, которые создаются на время сессии или текущего запроса
-- автоматически удаляются после завершения этой сессии/запроса

-- создание временной таблицы с кодом самолета, моделью самолета и количеством мест в нем
create temporary table AircraftModelSeatsAmountTemp (
	aircraft_code bpchar(3) primary key,
	model text not null unique,
	seats_amount integer not null
);

-- заполнение таблицы данными
insert into AircraftModelSeatsAmountTemp(aircraft_code, model, seats_amount)
values
	(773, 'Boeing 777-300', 550),
	(320, 'Airbus A320-200', 610),
	(733, 'Boeing 737-300', 490);

select * from AircraftModelSeatsAmountTemp;

-- join c временной таблицей
select
	a.aircraft_code,
	a.model,
	t.seats_amount
from bookings.aircrafts a
left join AircraftModelSeatsAmountTemp t
	on t.aircraft_code = a.aircraft_code;

-- удаление временной таблицы
drop table AircraftModelSeatsAmountTemp;