# задача 1
# with open('files10/filename.txt', 'r', encoding='utf-8') as file:
#     content_file = file.readlines()
#
#     posts = dict()
#     cities = dict()
#     countries = dict()
#
#     for i in content_file:
#         data = i.strip().split(', ')
#         if data[5] in posts.keys():
#             posts[data[5]] += 1
#         else:
#             posts[data[5]] = 1
#         if data[6] in cities.keys():
#             cities[data[6]] += 1
#         else:
#             cities[data[6]] = 1
#         if data[7] in countries.keys():
#             countries[data[7]] += 1
#         else:
#             countries[data[7]] = 1
#
# with open('files10/statistics.txt', 'w', encoding='utf-8') as file:
#     file.write('Статистика по профессиям:\n')
#     for post, count in posts.items():
#         file.write(f'{post}: {count} человек\n')
#     file.write('\nСтатистика по городам:\n')
#     for city, count in cities.items():
#         file.write(f'{city}: {count} человек\n')
#     file.write('\nСтатистика по странам:\n')
#     for country, count in countries.items():
#         file.write(f'{country}: {count} человек\n')

# задача 2
# with open('files10/students.txt', 'r', encoding='utf-8') as file:
#     lines = file.readlines()
#
#     students = {}
#
#     for line in lines:
#         data = line.strip().split(', ')
#         students[data[0]] = data[1]
#
# with open('files10/grades.txt', 'r', encoding='utf-8') as file:
#     lines_grades = file.readlines()
#
#     grades = {}
#
#     for line_grades in lines_grades:
#         data_grades = line_grades.strip().split(', ')
#         grades[data_grades[0]] = [int(x) for x in data_grades[1].split(' ')]
#
# student_info = []
# group_scores = {}
# for student in students.keys():
#     if student in grades.keys():
#         scores = grades[student]
#         average_score = sum(scores) / len(scores)
#         group = students[student]
#         student_info.append(f'{student}, {group}, {scores}, Средняя оценка: {average_score}')
#
#         if group not in group_scores.keys():
#             group_scores[group] = []
#             group_scores[group].append(average_score)
#         else:
#             group_scores[group].append(average_score)
#
# group_average_scores = {}
# for gr, av_sc in group_scores.items():
#     group_average_scores[gr] = sum(av_sc) / len(av_sc)
# sorted_group_average_scores = dict(sorted(group_average_scores.items(), key=lambda x : x[1]))
#
# with open('files10/report.txt', 'w', encoding='utf-8') as file:
#     for line in student_info:
#         file.write(line + '\n')
#     file.write('\nСредние оценки по группам:\n')
#     for group, average_score in sorted_group_average_scores.items():
#         file.write(f'{group}: {average_score:.2f}\n')

# задача 3
# try:
#     file =  open('files10/transactions.txt', 'r', encoding='utf-8')
#     lines = file.readlines()
#
#     transactions = {}
#
#     for line in lines:
#         transaction = line.strip().split(':')
#         name = transaction[0]
#         sum_transaction = int(transaction[1])
#         if name not in transactions.keys():
#             transactions[name] = sum_transaction
#         else:
#             transactions[name] += sum_transaction
# except FileNotFoundError as e:
#     print('Файл не обнаружен')
# finally:
#     if 'file' in locals():
#         file.close()
#
# with open('files10/accounts.txt', 'r', encoding='utf-8') as file:
#     lines_accounts = file.readlines()
#
#     accounts = {}
#
#     for line_accounts in lines_accounts:
#         account = line_accounts.strip().split(':')
#         accounts[account[0]] = int(account[1])
#
# try:
#     for name in accounts.keys():
#         if name in transactions.keys():
#             accounts[name] += transactions[name]
# except NameError as e:
#     print('Сумма на счетах не изменится, так как файл с транзакциями не обнаружен')
#
# with open('files10/accounts.txt', 'a', encoding='utf-8') as file:
#     file.write('\n\nИтоговое значение счета:\n')
#     for name, sum_account in accounts.items():
#         file.write(f'{name}: {sum_account}\n')