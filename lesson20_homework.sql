-- Задание 3.1: Анализ задержек рейсов по аэропорту
-- Для указанного аэропорта (по коду аэропорта) вывести статистику задержек.
select
	departure_airport,
	min(extract(epoch from actual_departure - scheduled_departure)) as min_delay,
	max(extract(epoch from actual_departure - scheduled_departure)) as max_delay,
	round(avg(extract(epoch from actual_departure - scheduled_departure)), 2) as avg_delay,
	count (*) filter (where actual_departure is not null) as delay_count
from bookings.flights
group by departure_airport
having departure_airport = 'DME';

-- Задание 3.2: Обернуть в функцию c вводом кода аэропорта
create or replace function bookings.flights_delay(code bpchar(3))
returns jsonb as $$
declare
	result jsonb;
begin
	select
		jsonb_build_object(
			'min_delay', round(min(extract(epoch from actual_departure - scheduled_departure)), 2),
			'max_delay', round(max(extract(epoch from actual_departure - scheduled_departure)), 2),
			'avg_delay', round(avg(extract(epoch from actual_departure - scheduled_departure)), 2),
			'delay_count', count (*) filter (where actual_departure is not null)
		)
	into result
	from bookings.flights
	where departure_airport = flights_delay.code;
	return result;
end;
$$ language plpgsql;

select bookings.flights_delay('DME');

-- Задание 4.1: Рейсы с заполняемостью выше средней
-- Задача: Найти рейсы, где количество проданных билетов превышает среднее по всем рейсам.
with avg_tickets_count as (
	select
		round(avg(tf.cnt_tickets), 2) as avg_cnt_tickets
	from (
		select
		flight_id,
		count(*) as cnt_tickets
		from bookings.ticket_flights
		group by flight_id
		order by flight_id
	) as tf
)
select
	tf.flight_id,
	f.flight_no,
	tf.cnt_tickets,
	atc.avg_cnt_tickets
from bookings.flights f
join (
	select
		flight_id,
		count(*) as cnt_tickets
	from bookings.ticket_flights
	group by flight_id
	order by flight_id
) as tf on tf.flight_id = f.flight_id
cross join avg_tickets_count atc
where tf.cnt_tickets > atc.avg_cnt_tickets;

-- Задание 4.2: Создать функцию с вводом параметра минимального процента заполняемости и выводом всех рейсов удовлетворяющих этому проценту
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

select * from bookings.for_flight_occupacy_rate

create or replace function bookings.flight_occupacy_rate(occupacy_rate numeric)
returns table (
	flight_id integer,
	cnt_tickets integer,
	seats_count integer,
	occ_rate numeric
) as $$
begin
	return query
	select
		f.flight_id,
		f.cnt_tickets,
		f.seats_count,
		f.occ_rate
	from bookings.for_flight_occupacy_rate f
	where f.occ_rate >= flight_occupacy_rate.occupacy_rate;
end;
$$ language plpgsql;


-- ЦИКЛЫ
-- FOREACH - для прохода по массиву
-- 	создание массива nums с числами от 1 до 5
-- 	FOREACH перебирает каждый элемент массива и выводит его с помощью RAISE NOTICE
DO $$
DECLARE
    num INTEGER;
    nums INTEGER[] := ARRAY[1, 2, 3, 4, 5];
BEGIN
    FOREACH num IN ARRAY nums
    LOOP
        RAISE NOTICE 'Число: %', num;
    END LOOP;
END;
$$ language plpgsql;

-- WHILE - для выполнения действия до тех пор, пока не будет выполнено условие
DO $$
DECLARE
    sum INTEGER := 0;
    i INTEGER := 1;
BEGIN
    WHILE i <= 5 LOOP
        sum := sum + i;  -- добавляем i к сумме
        i := i + 1;      -- увеличиваем i на 1
    END LOOP;
    RAISE NOTICE 'Сумма чисел от 1 до 5: %', sum;
END;
$$ language plpgsql;