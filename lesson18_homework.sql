-- Задача 1: Вывести аэропорты, из которых выполняется менее 50 рейсов
select
	a.airport_code,
	a.airport_name,
	count(f.flight_id) as flights_count
from bookings.airports a
inner join bookings.flights f
	on f.departure_airport = a.airport_code
group by a.airport_code 
having count(f.flight_id) < 50
order by count(f.flight_id)
;

-- Задача 2: Вывести среднюю стоимость билетов для каждого маршрута (город вылета - город прилета)
select
	concat(a_dep.city, ' - ', a_arr.city) as routes,
	round((avg(amount)), 2) as avg_ticket_amount
from bookings.flights f
inner join bookings.airports a_dep
	on a_dep.airport_code = f.departure_airport
inner join bookings.airports a_arr
	on a_arr.airport_code = f.arrival_airport
inner join bookings.ticket_flights tf
	on tf.flight_id = f.flight_id
group by routes
order by routes
;

-- Задача 3: Вывести топ-5 самых загруженных маршрутов (по количеству проданных билетов)
select
	concat(a_dep.city, ' - ', a_arr.city) as routes,
	count(ticket_no ) as ticket_count
from bookings.flights f
inner join bookings.airports a_dep
	on a_dep.airport_code = f.departure_airport
inner join bookings.airports a_arr
	on a_arr.airport_code = f.arrival_airport
inner join bookings.ticket_flights tf
	on tf.flight_id = f.flight_id
group by routes
order by ticket_count desc
limit 5
;