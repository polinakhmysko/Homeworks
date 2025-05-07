create schema studyings;

create table teachers (
	teacher_id integer primary key,
	teacher_name text not null,
	teacher_email text not null
);

create table students (
	student_id integer primary key,
	student_name text not null,
	student_email text not null
);

create table courses (
	course_id integer primary key,
	course_name text not null,
	category text not null,
	teacher_id integer not null,
	foreign key (teacher_id) references teachers(teacher_id)
);

create table moduls (
	modul_id integer primary key,
	modul_name text not null,
	course_id integer not null,
	foreign key (course_id) references courses(course_id)
);

create table lessons (
	lesson_id integer primary key,
	lesson_topic text not null,
	modul_id integer not null,
	foreign key (modul_id) references moduls(modul_id)
);

create table course_registration (
	course_registration_id SERIAL primary key,
	student_id integer not null,
	course_id integer not null,
	date_of_registration date not null,
	foreign key (student_id) references students(student_id),
	foreign key (course_id) references courses(course_id)
	
);

create table homeworks (
	homework_id SERIAL primary key,
	lesson_id integer not null,
	hometask text not null,
	foreign key (lesson_id) references lessons(lesson_id)
);

create table homework_checks (
	homework_check_id SERIAL primary key,
	homework_id integer not null,
	student_id integer not null,
	teacher_id integer not null,
	grade integer not null,
	foreign key (homework_id) references homeworks(homework_id),
	foreign key (student_id) references students(student_id),
	foreign key (teacher_id) references teachers(teacher_id)
);

create table reviews (
	review_id SERIAL primary key,
	course_id integer not null,
	student_id integer not null,
	review_grade integer not null,
	review text,
	foreign key (course_id) references courses(course_id),
	foreign key (student_id) references students(student_id)
);

-- Связи:
-- - таблицы teachers и courses: тип - 1:М (преподаватель может вести несколько курсов),
-- - таблицы courses и moduls: тип - 1:М (курс состоит из нескольких модулей),
-- - таблицы moduls и lessons: тип - 1:М (модуль содержит несколько уроков),
-- - таблицы homeworks и lessons: тип - 1:1 (урок содержит домашнее задание),
-- - таблицы students и courses через таблицу course_registration: тип - М:М 
--		(студент может записаться на несколько курсов / на курс могут записаться несколько студентов),
-- - таблицы students и homeworks через таблицу homework_checks: тип - М:М
-- 		(студент выполняет несколько домашних заданий / домашнее задание выполняют несколько студентов),
-- - таблицы teachers и homeworks через таблицу homework_checks: тип - 1:М
-- 		(преподаватель проверяет несколько домашних заданий),
-- - таблицы students и reviews: тип - 1:М (студент может оставить несколько отзывов),
-- - таблицы courses и reviews: тип - 1:М (курс может иметь несколько отзывов)