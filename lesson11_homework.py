# задача 1
def average_value(*args):
    try:
        av = sum(args) / len(args)
    except TypeError:
        print('Некорректный тип данных')
    except ZeroDivisionError:
        print('Данные не заданы')
    else:
        print(av)

if __name__ == '__main__':
    average_value(1,5,12,56,123)
    average_value(8,-23,125,2,-4,0)
    average_value('hi',2,'hello',1)
    average_value()

# задача 2
'''
функция, в которой в качестве параметров используем название файла с исходными данными
и название файла, в который будет записан результат
'''
def even_or_odd(filename, filename_result):
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            data = file.read()
    except FileNotFoundError:# проверка на наличие файла с исходными данными
        print('Файл не найден')
    else:
        list_num = [int(x) for x in data.split(',')]#превращение строки в список чисел
        odd_or_even_numbers = {}
        for num in list_num:#проверка на четность и запись пар(число: четное/нечетное) в созданный словарь
            if num % 2 == 0:
                odd_or_even_numbers[num] = 'Четное'
            else:
                odd_or_even_numbers[num] = 'Нечетное'
        with open(filename_result, 'w', encoding='utf-8') as file:
            for num, value in odd_or_even_numbers.items():
                file.write(f'{num} : {value}\n')

if __name__ == '__main__':
    even_or_odd('numbers.txt', 'numbers_result.txt')

# задача 3
class Transaction:
    def __init__(self, tr_type, summa, date):
        self.tr_type = tr_type
        self.summa = summa
        self.date = date
    def __str__(self):
        return f'Транзакция: тип - {self.tr_type}, сумма - {self.summa}, дата - {self.date}'

class Account:
    def __init__(self, num_account, balance):
        self.num_account = num_account
        self.balance = balance
        self.transactions = []
    def __str__(self):
        return f'Счет: номер - {self.num_account}, баланс - {self.balance}'

    def add_transaction(self, transaction):
        """
        создание метода на добавление суммы транзакции к балансу либо вычитание суммы транзакции из баланса,
        а также добавление самой транзакции в список транзакций
        """
        if transaction.tr_type == 'доход':
            self.balance += transaction.summa
        elif transaction.tr_type == 'расход':
            if transaction.summa <= self.balance:
                self.balance -= transaction.summa
            else:
                raise Exception('Недостаточно средств на счете')
        self.transactions.append(transaction)

    def get_balance(self):
        return self.balance

class Client:
    def __init__(self, name, client_id):
        self.name = name
        self.client_id = client_id
        self.accounts = []
    def __str__(self):
        return f'Клиент: имя - {self.name}, ID - {self.client_id}'

    def add_account(self, account):
        self.accounts.append(account)

    def get_total_balance(self):
        """
        метод возвращает общий баланс по всем счетам клиента,
        используя метод получения баланса по отдельному счету,
        проходясь по всем счетам в списке счетов отдельного клиента
        """
        return sum(account.get_balance() for account in self.accounts)

class Bank:
    def __init__(self):
        self.clients = []

    def add_client(self, client):
        return self.clients.append(client)

    def find_client_by_id(self, client_id):
        for client in self.clients:
            if client.client_id == client_id:
                return client
            else:
                return 'Клиент не найден'

    def client_report(self, client_id, filename):
        for client in self.clients:
            if client.client_id == client_id:
                report = f'Отчет для клиента: {client.name}, ID: {client.client_id}\n'
                report += f'Общий баланс по всем счетам: {client.get_total_balance()}\n'
                for account in client.accounts:
                    report += f'\n{account}\n'
                    for transaction in account.transactions:
                        report += f'    {transaction}\n'
                with open(filename, 'w', encoding='utf-8') as file:
                    file.write(report)
            else:
                with open(filename, 'w', encoding='utf-8') as file:
                    file.write('Клиент не найден')

if __name__ == '__main__':
    tr1 = Transaction('доход', 1500, '2025.03.12')
    tr2 = Transaction('расход', 2500, '2025.03.14')
    ac1 = Account(1234, 2000)
    ac2 = Account(1235, 3400)
    ac1.add_transaction(tr1)
    ac1.add_transaction(tr2)
    cl1 = Client('Polina', 100)
    cl1.add_account(ac1)
    cl1.add_account(ac2)
    bank1 = Bank()
    bank1.add_client(cl1)
    print(bank1.find_client_by_id(100))
    bank1.client_report(100, 'client100.txt')
    bank1.client_report(101, 'client101.txt')