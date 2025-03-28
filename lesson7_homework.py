# задача 1
import sys
array_str = sys.argv[1]
array = array_str.split(',')
array = [int(x) for x in array]
for x in array:
    if x < 10:
        print(f'{x} - Маленькое')
    elif 10 <= x <= 50:
        print(f'{x} - Среднее')
    else:
        print(f'{x} - Большое')
#
# задача 2
# import sys
# numbers_str = sys.argv[1]
# numbers = numbers_str.split(',')
# numbers = [int(x) for x in numbers]
# min_x = numbers[0]
# for x in numbers:
#     if x < min_x:
#         min_x = x
# print(f'Минимальное значение: {min_x}')
#
# задача 3
# import sys
# list1_str = sys.argv[1]
# list2_str = sys.argv[2]
# list1 = list1_str.split(',')
# list1 = [int(x) for x in list1]
# list2 = list2_str.split(',')
# list2 = [int(j) for j in list2]
# common_x = None
# for x in list1:
#     if x in list2:
#         common_x = x
#         break
# print(f'Первый общий элемент: {common_x}' if common_x != None else 'Общих элементов нет')