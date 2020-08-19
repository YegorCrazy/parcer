import requests
from bs4 import BeautifulSoup #еще нужна lxml

import time
import datetime
import sys
import json

import pymysql
import cryptography

with open('config.json') as config_file:
    try:
        config = json.load(config_file)
    except json.decoder.JSONDecodeError: #открываем файл конфигурации
        print('С файлом config.json что-то не так.')
        sys.exit()

conn = pymysql.connect(host='localhost', port=3306, 
  user=config['usr'], passwd=config['pwd'], db=config['database']) #подключение к базе данных
cursor = conn.cursor(pymysql.cursors.DictCursor)

gen_link = 'https://moscow.birge.ru/catalog/sdacha_sdam-koiko-mesto/?PAGEN_1=' #дефолтная ссылка на страницу-ленту 
flag = True #номер страницы
page_num = 1
gen_link += str(page_num)

cur_datetime = datetime.datetime.now()
#table_name = str(cur_datetime.day) + '.' + str(cur_datetime.month) + '.' + str(cur_datetime.year) + '_' + str(cur_datetime.hour) + ':' + str(cur_datetime.minute)
table_name = cur_datetime.strftime("%d_%m_%Y_%H_%M")
print("Таблица: " + table_name)
request = "CREATE TABLE IF NOT EXISTS  " + table_name + " (link TEXT, number TEXT, name TEXT, description TEXT, person TEXT, phone TEXT, location TEXT, email TEXT, publication_date TEXT, pictures TEXT)"
cursor.execute(request)

num = 0 #номер объявления

while flag == True:
    
    print()
    print('Страница номер ' + str(page_num))
    print()
    
    try:
        r = requests.get(gen_link) #подключение к странице-ленте
    except requests.exceptions.TooManyRedirects:
        print("Страница недоступна!")
        sys.exit()
    #print(r.text)

    soup = BeautifulSoup(r.text, 'html.parser') #превращаем html в текст

    table = soup.find("div", class_ = "listitem_catalog")
    res = table.find_all('div', class_='catalog_item') #ищем объекты со ссылками на страницы
    #res = soup.find("h5", id="listing-card-list")
    #print(res)

    session = requests.Session()
    session.max_redirects = 30
    session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36' #для адекватного подключения

    for i in res:
        
        num+=1

##        if num > 4:
##            cursor.close()
##            conn.close()
##            sys.exit()
        
        print()
        
        data = i.find('a', class_ = 'href-detail')
        link = 'https://moscow.birge.ru' + data.get('href') #находим ссылку на страницу и переходим по ней
        print(link)
        print()
        
        try:
            page_r = session.get(link, allow_redirects=True)
##            print(page_r)
            page = BeautifulSoup(page_r.text, 'html.parser')
            
            print("Название:", end=' ')
            title = page.h1.text
            print(title)
            print()
            
            print("Описание:", end=' ')
            desc = page.find('div', class_ = 'ads_field').text
            print(desc)
            print()

            contact = page.find('div', class_ = 'contact')
            name_child = contact.find('i', class_ = 'fa-user')
            name = name_child.parent.text
            phone = 'https://moscow.birge.ru' + contact.find('img', class_ = 'dont_copy_phone').get('src') #телефон почему-то картинкой

            print('Контактное лицо: ' + name) #обязательные данные (вроде)
            print()
            print('Телефон: ' + phone)
            print()

            location = contact.find_all('i', class_ = 'metro')
            metro = ''
            if len(location)>0:
                metro = location[0].parent.text
                print('Метро: ' + metro)
                print()

            mail = contact.find_all('i', class_ = 'fa-envelope') #необязательные данные
            adress = ''
            if len(mail)>0:
                adress = mail[0].parent.text
                print('Мейл: ' + adress)
                print()

            locate = page.find('div', class_ = 'locate')
            date_plus = locate.find_all('div', class_ = 'city-date')
            date = date_plus[1].text
            print('Дата публикации: ' + date)
            print()

            right_side = contact.parent
            pictures = right_side.find_all('a', class_ = 'fancybox-buttons')
            photo_links = ''
            if len(pictures)>0:
                print('Ссылки на фотографии:')
                for i in pictures:
                    print('https://moscow.birge.ru' + i.get('href'))
                    photo_links += ('https://moscow.birge.ru' + i.get('href') + ' ')
                print()
            
            print('---------------------------------------------------------')

            cursor.execute("INSERT INTO " + table_name + " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (link, num, title, desc, name, phone, metro, adress, date, photo_links))
            conn.commit() #пишем в базу данных
            
        except requests.exceptions.TooManyRedirects: #на случай перенаправления по кругу
            print("Редиректы...")

        time.sleep(5)

    next_page = soup.find_all('a', class_ = 'modern-page-next')
    if len(next_page) > 0:
        gen_link = 'https://moscow.birge.ru' + next_page[0].get('href') #поиск следующей страницы денты
        page_num+=1
    else:
        flag = False

cursor.close()
conn.close()
