import csv
import json
import os
from filelock import FileLock
import multiprocessing
import re
import string
import time
from pprint import pprint

import requests
from bs4 import BeautifulSoup

from utils import write_json


headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36',
}

ROOT_URL = 'https://maxmaster.ru'
OUTPUT_FILE_PATH = '../zubr_parsing_results.csv'
OUTPUT_FILELOCK_PATH = '../zubr_parsing_results.csv.lock'


def get_html(url, params=None):
    r = requests.get(url, headers=headers, params=params)
    print(f'{r.status_code} | {r.url}')
    return r.text


def write_to_csv(data, flag='a'):
    # with open(OUTPUT_FILE_PATH, flag, newline='', encoding='utf-8-sig') as f:
    #     writer = csv.writer(f, delimiter=';', dialect='excel', quoting=csv.QUOTE_MINIMAL)
    #     writer.writerow(data)
    lock = FileLock(OUTPUT_FILELOCK_PATH)
    with lock:
        with open(OUTPUT_FILE_PATH, flag) as f:
            writer = csv.writer(f)
            writer.writerow(data)



def select_categories_to_parse():
    url = ROOT_URL + '/index.php?dispatch=categories.ab__lc_catalog'
    soup = BeautifulSoup(get_html(url), 'lxml')
    categories = [
        {'name': div.a.text.strip(), 'url': div.a.get('href')} 
        for div in soup.find_all('div', class_='cat-title')
    ]

    os.system('cls' if os.name == 'nt' else 'clear')

    print(f'[Парсинг {ROOT_URL}]\n')
        
    # Отрисовка основных категорий
    print('0 - Все')
    for i, category in enumerate(categories):
        print(f'{i + 1} - {category["name"]}')

    # Ввод основных категорий для парсинга по ним
    done = False
    while not done:
        selected_indices_categories = input('\nВведиде номера категорий (через пробел): ').split()
        selected_categories_urls = set()

        for i in selected_indices_categories:
            try:
                index = int(i)
                if index == 0:
                    done = True
                    selected_categories_urls = [category['url'] for category in categories]
                    break
                if not (0 < index <= len(categories)):
                    print('\nВыберите номера из списка.')
                    break
            except ValueError:
                print('\nНеобходимо ввести числовое значение.')
                break
            else:
                selected_categories_urls.add(categories[index - 1]['url'])
        else:
            done = True

    return list(selected_categories_urls)



def get_products_urls(category_url):
    soup = BeautifulSoup(get_html(category_url + 'zubr/?items_per_page=100000'), 'lxml')
    content = soup.find(id='categories_view_pagination_contents')
    products_urls = [
        div.a.get('href') 
        for div in content.find_all('div', class_='ut2-gl__image')
    ]
    return products_urls


def get_product_data(product_url):
    content = BeautifulSoup(get_html(product_url), 'lxml')

    try:
        title = content.find('meta', {'itemprop': 'name'}).get('content')
    except:
        title = ''

    try:
        routes = [
            a.text.strip()
            for a in content.find_all('a', class_='ty-breadcrumbs__a')
        ]
    except:
        routes = []

    try:
        product_code = content.find('meta', {'itemprop': 'sku'}).get('content')
    except:
        product_code = ''

    try:
        div = content.find('div', class_='ut2-pb__note')
        guarantee = div.text.replace('Гарантия производителя -', '').strip()
    except:
        guarantee = ''

    try:
        price = content.find('meta', {'itemprop': 'price'}).get('content')
    except:
        price = ''

    try:
        description = content.find('meta', {'itemprop': 'description'}).get('content')
    except:
        description = ''

    try:
        div = content.find('div', class_='harakteristiki_tovara')
        characteristics = []
        for c in div.find_all('div', class_='ty-product-feature'):
            key = c.find('span', class_='ty-product-feature__label').text.strip()
            value = c.find('span', class_='ac24_feature__value').text.strip()
            characteristics.append(f'{key} {value}')
        characteristics = '\n'.join(characteristics)
    except:
        characteristics = ''

    try:
        div = content.find('div', class_='span8 osnovnie_dannie')
        equipment = div.find('li').text.strip()
    except:
        equipment = ''

    try:
        ul = content.find('ul', class_='gabariti_i_ves')
        dimensions_and_weight = []
        for li in ul.find_all('li'):
            dimensions_and_weight.append(' '.join(li.text.replace('\n', '').split()))
        dimensions_and_weight = '\n'.join(dimensions_and_weight)
    except:
        dimensions_and_weight = ''

    try:
        documentation = '\n'.join([
            ROOT_URL + li.a.get('href')
            for li in content.find('ul', class_='sertificati').find_all('li')
        ])
    except:
        documentation = ''

    try:
        urls_photos = [
            meta.get('content')
            for meta in content.find_all('meta', {'itemprop': 'image'})
        ]
    except:
        urls_photos = []

    product_data = {
        'product_code': f'="{product_code}"',
        'title': title,
        'route': '/'.join(routes),
        'brand': 'Зубр',
        'guarantee': guarantee,
        'price': f'="{price}"',
        'description': description,
        'characteristics': characteristics,
        'equipment': equipment,
        'dimensions_and_weight': dimensions_and_weight,
        'documentation': documentation,
        'urls_photos': urls_photos
    }

    return product_data


def write_product_data(product_url):
    product_data = get_product_data(product_url)
    write_to_csv((
        product_data['title'],
        product_data['route'],
        product_data['product_code'],
        product_data['brand'],
        product_data['guarantee'],
        product_data['price'],
        product_data['description'],
        product_data['characteristics'],
        product_data['equipment'],
        product_data['dimensions_and_weight'],
        product_data['documentation'],
        *product_data['urls_photos'],
    ))
        


def main():
    # Просим пользователя выбрать, какие категории парсить
    selected_categories_urls = select_categories_to_parse()

    print('\n[Начало сбора данных]\n')

    start_point = time.time()

    # Достаём все ссылки на продукты указанных категорий    
    products_urls = []
    for url in selected_categories_urls:
        products_urls += get_products_urls(url)

    # Записываем хедер
    write_to_csv(
        (
            'Название',
            'Путь',
            'Артикул',
            'Бренд',
            'Гарантия производителя',
            'Цена',
            'Описание',
            'Характеристики',
            'Комплектация',
            'Габариты и вес в упаковке',
            'Документация',
            'Ссылки на фотографии',
        ), 
        'w'
    )

    # Параллельно записываем данные от каждом продукте
    with multiprocessing.Pool(40) as p:
        p.map(write_product_data, products_urls)

    end_point = time.time()

    print(f'\n[Время выполнения - {(end_point - start_point) / 60} мин.]')
    input(f'\n[Конец сбора данных][Нажмите любую клавишу, чтобы выйти]')
        

if __name__ == '__main__':
    main()