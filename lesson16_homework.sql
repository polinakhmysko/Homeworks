CREATE table promocodes (
	promo_id integer PRIMARY KEY AUTOINCREMENT,
	code text NOT NULL UNIQUE,
	discount_percent integer check(discount_percent between 1 and 100),
	valid_from DATE,
	valid_to DATE,
	max_uses integer check(max_uses > 0 or max_uses IS NULL),
	used_count integer DEFAULT 0,
	is_active integer DEFAULT 1 check(is_active in (0, 1)),
	created_by integer NOT NULL,
	
	FOREIGN KEY (created_by) REFERENCES users(id)
);

INSERT into promocodes (code, discount_percent, valid_from, valid_to, max_uses, used_count, is_active, created_by)
VALUES
	('SUMMER10', 10, '2025-04-01', '2025-08-31', 100, 4, 1, 23),
    ('WELCOME20', 20, '2025-01-01', '2025-04-30', NULL, 3, 1, 20),
    ('BLACKFRIDAY30', 30, '2024-11-24', '2024-11-27', 500, 1, 0, 14),
    ('NEWYEAR15', 15, '2024-12-20', '2025-01-10', 200, 4, 0, 18),
    ('FLASH25', 25, '2025-05-01', '2025-07-01', 50, 0, 0, 16),
    ('LOYALTY5', 5, '2025-01-01', '2026-01-01', NULL, 5, 1, 22),
    ('MEGA50', 50, '2025-09-01', '2025-09-30', 10, 0, 0, 15),
    ('AUTUMN20', 20, '2024-09-01', '2024-11-30', NULL, 6, 0, 19),
    ('SPRING10', 10, '2025-03-01', '2025-05-31', 150, 4, 1, 17),
    ('VIP40', 40, '2025-04-01', '2025-04-30', 20, 1, 1, 21);

select * from promocodes;

-- Анализ по группам скидок
-- Сгруппировать промокоды по диапазонам скидок и вывести:
-- - Количество промокодов в группе.
-- - Минимальную и максимальную скидку.
-- - Сколько из них имеют ограничение по использованию (max_uses IS NOT NULL).

SELECT
	CASE 
		when discount_percent between 1 and 20 then '1% - 20%'
		when discount_percent between 21 and 40 then '21% - 40%'
		when discount_percent between 41 and 60 then '41% - 60%'
		else '> 60%'
	END as discount_percent_group,
	count(*) as promocode_count,
	min(discount_percent) as min_discount_percent,
	max(discount_percent) as max_discount_percent,
	sum(CASE WHEN max_uses IS NOT NULL THEN 1 ELSE 0 END) as with_max_uses
from promocodes
group by discount_percent_group;

-- Анализ по времени действия
-- Разделить промокоды на:
-- - Активные (текущая дата между valid_from и valid_to).
-- - Истекшие (valid_to < текущая дата).
-- - Еще не начавшиеся (valid_from > текущая дата).
-- Для каждой группы вывести:
-- - Количество промокодов.
-- - Средний процент скидки.
-- - Сколько из них имеют лимит использований.

SELECT
	CASE 
		when valid_from < CURRENT_DATE and valid_to > CURRENT_DATE then 'Активные'
		when valid_to < CURRENT_DATE then 'Истекшие'
		when valid_from > CURRENT_DATE then 'Еще не начавшиеся'
		else 'Не определено'
	END as valid_promocode_group,
	count(*) as promocode_count,
	avg(discount_percent) as avg_discount_percent,
	sum(CASE WHEN max_uses IS NOT NULL THEN 1 ELSE 0 END) as with_max_uses
from promocodes
group by valid_promocode_group;