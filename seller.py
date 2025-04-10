import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """Получить список товаров магазина озон

    Отправка запроса к Ozon Api для получения списка всех товаров.
    Args:
        last_id (str): Идентификатор последнего товара для постраничного доступа.
        client_id (str): id клиента, нужен для авторизации.
        seller_token (str): Токен, необходимый для доступа к API.

    Returns:
        list: Список со всеми товарами.

    Raises:
        AttributeError: Если атрибуты last_id, client_id, seller_token
                        не строчного типа данных.
        HTTPError: При неуспешной попытке передать запрос
                   либо при неуспешной попытке получения ответа от Api.

    Examples:
        >>> get_product_list('', 'client_id', 'seller_token')
        "items": [{"archived": true ... }]}
        >>> get_product_list('', 'incorrect_client_id', 'incorrect_seller_token')
        None
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить артикулы товаров магазина озон

    Args:
        client_id (str): id клиента, нужен для авторизации.
        seller_token (str): Токен, необходимый для доступа к API.

    Returns:
        list: Список артикулов товаров.

    Raises:
        AttributeError: Если атрибуты client_id, seller_token
                        не строчного типа данных.
        HTTPError: При неуспешной попытке передать запрос
               либо при неуспешной попытке получения ответа от Api.

    Examples:
        >>> get_offer_ids('client_id', 'seller_token')
        ['136748', '321456', '236654', ...]

        >>> get_offer_ids('client_id', 'incorrect_token')
        None
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров

    Args:
        prices (list): Список цен, для обновления в магазине.
        client_id (str): id клиента, нужен для авторизации.
        seller_token (str): Токен, необходимый для доступа к API.

    Returns:
        dict: Ответ от API, со статусом запроса на обновление.

    Raises:
        AttributeError: Если атрибуты last_id, client_id, seller_token
                        не строчного типа данных.
        HTTPError: При неуспешной попытке передать запрос
                   либо при неуспешной попытке получения ответа от Api.

    Examples:
        >>> update_price(price, 'client_id', 'seller_token')
        {'success': True, 'updated_count': 1}

        >>> update_price(price, 'client_id', 'incorrect_token')
        {"code": 0, "details": [{"typeUrl": "string","value": "string"}], "message": "string"}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки

    Args:
        stocks (list): Список остатков товаров.
        client_id (str): id клиента, нужен для авторизации.
        seller_token (str): Токен, необходимый для доступа к API.

    Returns:
        dict: Ответ от API, со статусом запроса на обновление.

    Raises:
        AttributeError: Если атрибуты last_id, client_id, seller_token
                        не строчного типа данных.
        HTTPError: При неуспешной попытке передать запрос
                   либо при неуспешной попытке получения ответа от Api.
    Examples:
        >>> update_stocks(stocks, client_id, seller_token)
        {"result": [{"product_id": 55946,"offer_id": "PG-2404С1", "updated": true, "errors": []}]}

        >>> update_stocks(stocks, client_id, incorrect_seller_token)
        {"code": 0,"details": [{"typeUrl": "string", "value": "string"}], "message": "string"}
    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл ostatki с сайта casio

    Returns:
        dict: Cловарь c информацией о часах.

    Raises:
        RequestException: если URL недоступен, нет интернета или
                          сервер вернул ошибку (404, 500 и т. д.)
        Exception: Общая ошибка, если возникли проблемы с загрузкой
                   или обработкой файла.
        PermissionError: если нет прав на запись в текущую папку.

    Examples:
        >>> dowload_stock()
            {'Код':73668, 'Наименование товара':'BA-110AQ-4A', 'Цена':'19'990.00 руб.'}
        >>> dowload_stock()
            requests.exceptions.HTTPError: 403 Client Error: Forbidden for url: https://timeworld.ru/upload/files/ostatki.zip
    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создать остатки товаров магазина Озон.

    Синхронизирует остатки часов с оптового магазина с магазином OZON.
    Товарам без остатка будет проставлен 0.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        offer_ids (list): Список артикулов товаров магазина Озон.

    Returns:
        list: Список артикулов товаров с остатками

    Raises:
        KeyError: Если ключи "Код", "Количество" отсутствуют в словаре watch

    Examples:
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        {"offer_id": offer_id, "stock": 0}
        >>> create_stocks(incorrect_watch_remnants, offer_ids, incorrect_warehouse_id)
        None
    """
    # Уберем то, что не загружено в seller
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать цены товаров магазина Озон.

    Синхронизирует цены часов с оптового магазина с магазином OZON.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        offer_ids (list): Список артикулов товаров магазина Озон.

    Returns:
        list: Список артикулов товаров с ценами

    Raises:
        KeyError: Если ключи "Код", "Цена" отсутствуют в словаре watch
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """Преобразовать цену.

    Убирает из передаваемой в нее строки все нецифровые символы.
    Десятичная часть числа убирается.

    Args:
        price (str): Строка с ценой товара.

    Returns:
        str: Строка содержащая отформатированную цену.

    Raises:
        AttributeError: Если price не строчного типа данных.

    Examples:
        >>> print(price_conversion('19'990.00 руб.'))
        '19990'
        >>> price_conversion("X̅MX̅CMXC")
        ''
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделить список lst на части по n элементов
    Args:
        lst: Исходный список, который нужно разделить.
        n: Количество элементов в каждом подсписке. Должно быть > 0.

    Yields:
        Подсписок из `n` элементов.

    Raises:
        ValueError: Если `n` <= 0.
    """
    for i in range(0, len(lst), n):
        yield lst[i: i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """Загрузить цены товаров на сервер Озон.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        client_id (str): id клиента, нужен для авторизации.
        seller_token (str): Токен, необходимый для доступа к API.

    Returns:
        list: Список артикулов товаров с ценами.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """Загрузить остакти товаров на сервер Озон.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        client_id (str): id клиента, нужен для авторизации.
        seller_token (str): Токен, необходимый для доступа к API.

    Returns:
        tuple: Список артикулов товаров с информацией об остатках.
    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
