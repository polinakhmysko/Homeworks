# задача 1
a, b, c = float(input()), float(input()), float(input())
D = b**2 - 4*a*c
x1 = (-b + D**0.5)/(2*a)
x2 = (-b - D**0.5)/(2*a)
print(x1, x2, sep = '; ')

# задача 2
n = int(input('Введите трехзначное число: '))
n1  = n//100
n2 = (n%100)//10
n3 = n%10
s = n1+n2+n3
p = n1*n2*n3
print(s, p, sep = '; ')

# задача 3
login_correct = input('Правильный логин? (True/False): ') == 'True'
password_correct = input('Правильный пароль? (True/False): ') == 'True'
dostup = input('Есть специальный токен доступа? (True/False): ') == 'True'
print('Пользователь имеет доступ: ', (login_correct and password_correct) or dostup)

# задача 4
subs = input('Подписан на платную подписку? (True/False): ') == 'True'
age = input('Старше 18 лет? (True/False): ') == 'True'
adm = input('Администратор? (True/False): ') == 'True'
account = input('Аккаунт активный? (True/False): ') == 'True'
d = ((subs and age) or adm) and account
print('Пользователь имеет доступ: ', d)