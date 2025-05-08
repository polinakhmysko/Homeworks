-- Задача 1:
-- Оптимизировать выборку данных по номеру места (bookings.boarding_passes.seat_no) 
-- с помощью индекса и сравнить результаты до добавления индекса и после (время выполнения и объем таблицы в %)

-- Execution Time: 53.522 ms
-- объем таблицы - 80М
explain analyze
select
	seat_no
from bookings.boarding_passes
where seat_no = '2A';

-- создание индекса
create index idx_boarding_passes_seat_no on bookings.boarding_passes (seat_no);
-- Execution Time: 1.619 ms
-- объем таблицы - 84М
-- время выполнения уменьшилось на 97%, объем таблицы вырос на 5%

-- Задача 2:
-- 1. Проанализировать производительность запроса без индексов.
-- 2. Добавить индексы для ускорения JOIN и фильтрации.
-- 3. Снова проанализировать производительность запроса и сравнить результаты.
explain analyze
SELECT bp.boarding_no, t.passenger_id
FROM bookings.boarding_passes bp
JOIN bookings.tickets t ON bp.ticket_no = t.ticket_no
JOIN bookings.seats s ON bp.seat_no = s.seat_no
JOIN bookings.bookings b ON t.book_ref = b.book_ref
WHERE 
  t.passenger_id in ('0856 579180', '4723 695013')
  and bp.boarding_no < 100
;
-- Execution Time: 27.814 ms
-- объем таблицы tickets - 59М
-- объем таблицы boarding_passes - 84М

-- создание индексов
create index idx_boarding_passes_boarding_no on bookings.boarding_passes (boarding_no);
create index idx_tickets_passenger_id on bookings.tickets (passenger_id);
create index idx_boarding_passes_ticket_no on bookings.boarding_passes (ticket_no);
create index idx_boarding_passes_seat_no on bookings.boarding_passes (seat_no);
create index idx_tickets_book_ref on bookings.tickets (book_ref);
-- Execution Time: 2.637 ms
-- объем таблицы tickets - 77М
-- объем таблицы boarding_passes - 99М

-- время выполнения уменьшилось на 90,5%,
-- объем таблицы tickets вырос на 30,5%, таблицы boarding_passes - на 18%

-- созданаие частичных индексов (под условия запроса)
drop index bookings.idx_boarding_passes_boarding_no;
drop index bookings.idx_tickets_passenger_id;
create index idx_boarding_passes_boarding_no on bookings.boarding_passes (boarding_no) where boarding_no < 100;
create index idx_tickets_passenger_id on bookings.tickets (passenger_id) where passenger_id in ('0856 579180', '4723 695013');
-- Execution Time: 0.791 ms
-- объем таблицы tickets - 66М
-- объем таблицы boarding_passes - 99М

-- время выполнения уменьшилось на 97%,
-- объем таблицы tickets вырос на 12%, таблицы boarding_passes - на 18%