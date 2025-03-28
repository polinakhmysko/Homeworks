# задача 1
import sys
list1 = sys.argv[1]
list_numbers = [int(x) for x in list1.split(',')]
even_numbers = tuple(x for x in list_numbers if x%2==0)
odd_numbers = tuple(x for x in list_numbers if x%2!=0)
print(even_numbers, odd_numbers, sep=', ')

# задача 2
import sys
str_1 = sys.argv[1]
str_2 = sys.argv[2]
if len(str_1) != len(str_2):
    print('Нет, это не "Анаграмма"')
else:
    letters = dict()
    for i in str_1:
        if i in letters.keys():
            letters[i] += 1
        else:
            letters[i] = 1
    for i in str_2:
        if i in letters.keys():
            letters[i] -= 1
    letter = None
    ''' ввели данную переменную для выявления первого случая, 
    когда количество одинаковых букв в одной строке 
    не соответствует количеству этих букв в другой строке
    '''
    for i in letters.keys():
        if letters[i] != 0:
            letter = 1
            break
    print('Нет, это не "Анаграмма"' if letter != None else 'Да, это "Анаграмма"')