# задача 1
data_2023_01 = [
    "2023-01-01:1000:Иван Иванов",
    "2023-01-02:1500:Петр Петров",
    "2023-01-03:2000:Мария Сидорова"
]

data_2023_02 = [
    "2023-02-01:1200:Иван Иванов",
    "2023-02-02:1800:Петр Петров",
    "2023-02-03:2200:Мария Сидорова"
]

data_2023_03 = [
    "2023-03-01:1300:Иван Иванов",
    "2023-03-02:1700:Петр Петров",
    "2023-03-03:2100:Мария Сидорова"
]

def creat_files(file_name, data_file):
    with open(file_name, 'w', encoding='utf-8') as file:
        file.writelines(list(map(lambda x: x + '\n', data_file)))

creat_files('files10/sales_2023_01.txt', data_2023_01)
creat_files('files10/sales_2023_02.txt', data_2023_02)
creat_files('files10/sales_2023_03.txt', data_2023_03)

with open('files10/sales_2023_01.txt', 'r', encoding='utf-8') as file:
    lines_1 = file.readlines()

with open('files10/sales_2023_02.txt', 'r', encoding='utf-8') as file:
    lines_2 = file.readlines()

with open('files10/sales_2023_03.txt', 'r', encoding='utf-8') as file:
    lines_3 = file.readlines()

sum_sells = 0
managers = {}
def sells_managers(data):
    global sum_sells
    global managers
    for line in data:
        line_data = line.strip().split(':')
        name = line_data[2]
        summa_name = int(line_data[1])
        sum_sells += summa_name
        if name not in managers.keys():
            managers[name] = 0
            managers[name] += summa_name
        else:
            managers[name] += summa_name
    return sum_sells, managers

sells_managers(lines_1)
sells_managers(lines_2)
sells_managers(lines_3)

sorted_managers = list(sorted(managers.items(), key=lambda x : x[1], reverse=True))
best_manager = sorted_managers[0][0]

with open('files10/report.txt', 'w', encoding='utf-8') as file:
    file.write(f'Общая сумма продаж: {sum_sells}\n')
    file.write(f'Лучший менеджер: {best_manager}')


# задача 2
# with open('files10/raw_data.txt', 'r', encoding='utf-8') as file:
#     raw_data_lines = file.readlines()
#
# def clean(raw_data):
#     cleaned_data = []
#     for line in raw_data:
#         if 'ERROR' in line:
#             continue
#         else:
#             cleaned_data.append(line)
#     return cleaned_data
#
# cleaned_data = clean(raw_data_lines)
#
# with open('files10/cleaned_data.txt', 'w', encoding='utf-8') as file:
#     file.writelines(cleaned_data)