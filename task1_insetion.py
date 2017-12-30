
# coding: utf-8

# In[152]:

import pandas as pd
import pymysql
from pymysql import MySQLError

wayToFile = 'C:\\Programms\\Tasks\\orders.csv'

#считывание .csv файл при помощи pandas
def read_csv_file(wayToFile):
    dataFrame = pd.read_csv(wayToFile, sep = ',')
    return dataFrame

#отправка request запроса к MySQL серверу
def request_to_mysql(sql):
    try:
        connection = pymysql.connect(host='localhost',
                                 user='',
                                 password='',
                                 db='worktasks',
                                 charset='utf8mb4',
                                 autocommit=True,
                                 cursorclass=pymysql.cursors.DictCursor)
        curs = connection.cursor()
        curs.execute(sql)
        connection.close()
        return ('The SQL request was successful', curs)
    except MySQLError as e:
        return ('Got error', str(e.args[0]))

if __name__ == '__main__':
    #считывание данных из сsv файла
    dataFrame  = read_csv_file(wayToFile)

    #начало строки-запроса на вставку данных в таблицу
    sqlInsert = 'INSERT INTO orders VALUES '

    #цикл по строкам датафрейма
    for row in range(dataFrame.shape[0]):    
        #генерируем sql запрос на вставку данных 
        #формат запроса INSERT INTO orders VALUES (165, STR_TO_DATE('6/18/2015', '%m/%d/%Y') ) ...
        sqlInsert = sqlInsert + '(' + str(dataFrame['client_id'][row]) + ', STR_TO_DATE(\'' + dataFrame['purchase_date'][row] + '\', \'%m/%d/%Y\') ),'

    #избавляемся от лишней запятой в конце запроса
    sqlInsert = sqlInsert[0:-1]
    
    #отправляем INSERT-запрос
    resp = request_to_mysql(sqlInsert)
    
    print(resp)


# In[ ]:



