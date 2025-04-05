CREATE table books (
	book_id integer,
	book_title text,
	publication_year integer,
	author_id integer,
	number_of_copies integer,
	status text
);

CREATE table authors (
	author_id integer,
	author_name text,
	year_of_birth integer,
	country text,
	biography text
);

CREATE table readers (
	reader_id integer,
	reader_name text,
	library_card integer,
	registration_date date,
	phone_number text,
	email text
);

INSERT into authors (author_id, author_name, year_of_birth, country, biography)
VALUES
	(1, 'Альбер Камю', 1913, 'Алжир', 'французский философ, писатель, лауреат Нобелевской премии по литературе'),
	(2, 'Вирджиния Вулф', 1882, 'Великобритания', 'английская писательница, литературный критик'),
	(3, 'Оскар Уайльд', 1854, 'Ирландия', 'ирландский писатель и поэт'),
	(4, 'Федор Достоевский', 1821, 'Россия', 'русский писатель, философ и публицист'),
	(5, 'Фредрик Бакман', 1981, 'Швеция', 'шведский писатель');

INSERT into books (book_id, book_title, publication_year, author_id, number_of_copies, status)
VALUES
	(41, 'Белые ночи', 1848, 4, 2, 'на руках'),
	(21, 'Волны', 1931, 2, 1, 'доступна'),
	(51, 'Вторая жизнь Уве', 2012, 5, 2, 'доступна'),
	(31, 'Из глубины', 1905, 3, 1, 'на руках'),
	(42, 'Игрок', 1866, 4, 1, 'в ремонте'),
	(22, 'На маяк', 1927, 2, 3, 'доступна'),
	(11, 'Падение', 1956, 1, 1, 'на руках'),
	(32, 'Портрет Дориана Грея', 1890, 3, 2, 'доступна'),
	(12, 'Посторонний', 1942, 1, 1, 'в ремонте'),
	(23, 'Своя компната', 1929, 2, 2, 'на руках');

INSERT into readers (reader_id, reader_name, library_card, registration_date, phone_number, email)
VALUES
	(110, 'Татьяна Смирнова', 1101, '2023-03-01', '+375332369817', 't.smirnova@gmail.com'),
	(211, 'Мария Александрова', 2111, '2022-12-01', '+375339476210', 'm.aleksandrova@gmail.com'),
	(312, 'Оксана Степанова', 3121, '2023-05-01', '+375335091482', 'o.stepanova@gmail.com'),
	(413, 'Сергей Иванов', 4131, '2024-02-01', '+375331623849', 's.ivanov@gmail.com'),
	(514, 'Евгений Кузнецов', 5141, '2024-02-01', '+375339461205', 'e.kuznezov@gmail.com'),
	(615, 'Егор Антипов', 6151, '2024-07-01', '+375339472618', 'e.antipov@gmail.com');

SELECT * from books
	where author_id = 2;

SELECT * from books
	where author_id = 3;

SELECT * from books
	where status = 'доступна';

SELECT * from readers
	where registration_date BETWEEN '2022-01-01' AND '2023-12-31';

SELECT * from readers
	where registration_date BETWEEN '2024-01-01' AND '2024-12-31';