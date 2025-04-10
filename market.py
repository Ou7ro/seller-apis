import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """Получить список товаров с Яндекс Маркета.

    Args:
        page (str): Идентификатор страницы c результатами.
                    Если параметр не указан, возвращается первая страница.
        campaign_id (str): Идентификатор магазина в кабинете.
        access_token (str): Токен, необходимый для доступа к API.

    Returns:
        list: Информация о товарах в каталоге.

    Raises:
        AttributeError: Если атрибуты page, campaign_id, access_token не являются строками.
        HTTPError: При неуспешной попытке передать запрос
                   либо при неуспешной попытке получения ответа от Api.

    Examples:
        >>> get_product_list(page, campaign_id, access_token)
            "paging": { "nextPageToken": "string" ... }
        >>> get_product_list(page, incorrect_campaign_id, incorrect_access_token)
            None
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновить остатки товаров на сервере Яндекс Маркета.

    Args:
        stocks (list): Список остатков товаров.
        campaign_id (str): Идентификатор магазина в кабинете.
        access_token (str): Токен, необходимый для доступа к API.

    Returns:
        dict: Ответ от API, со статусом запроса на обновление.

    Raises:
        AttributeError: Если атрибуты stocks, campaign_id, access_token не являются строками.
        HTTPError: При неуспешной попытке передать запрос
                   либо при неуспешной попытке получения ответа от Api.

    Examples:
        >>> update_stocks(stocks, client_id, seller_token)
        {"status": "OK"}

        >>> update_stocks(stocks, client_id, incorrect_seller_token)
        {"status": "OK", "errors": [{"code": "string", "message": "string"}]}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновить цены товаров

    Args:
        prices (list): Список цен, для обновления в магазине.
        campaign_id (str): Идентификатор магазина в кабинете.
        access_token (str): Токен, необходимый для доступа к API.

    Returns:
        dict: Ответ от API, со статусом запроса на обновление.

    Raises:
        AttributeError: Если атрибуты client_id, seller_token
                        не строчного типа данных.
        HTTPError: При неуспешной попытке передать запрос
                   либо при неуспешной попытке получения ответа от Api.

    Examples:
        >>> update_price(price, 'client_id', 'seller_token')
        {"status": "OK"}

        >>> update_price(price, 'client_id', 'incorrect_token')
        {"status": "OK", "errors": [{"code": "string", "message": "string"}]}
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получить артикулы товаров Яндекс маркет

    Args:
        campaign_id (str): Идентификатор магазина в кабинете.
        market_token (str): Токен, необходимый для доступа к API.

    Returns:
        list: Список артикулов товаров.

    Raises:
        AttributeError: Если атрибуты campaign_id, market_token
                        не строчного типа данных.
        HTTPError: При неуспешной попытке передать запрос
               либо при неуспешной попытке получения ответа от Api.

    Examples:
        >>> get_offer_ids('your_client_id', 'your_seller_token')
        ['136748', '321456', '236654', ...]

        >>> get_offer_ids('your_client_id', 'incorrect_token')
        None
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создать остатки товаров магазина.

    Синхронизирует остатки часов с оптового магазина с Яндекс маркет.
    Товарам без остатка будет проставлен 0.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        offer_ids (list): Список артикулов товаров Яндекс маркет.
        warehouse_id (string): Идентификатор склада на Яндекс маркет.

    Returns:
        list: Список артикулов товаров с остатками

    Raises:
        KeyError: Если ключи "Код", "Количество" отсутствуют в словаре watch

    Examples:
        >>> create_stocks(watch_remnants, offer_ids, warehouse_id)
        {"sku": offer_id, "warehouseId": warehouse_id, ...}
        >>> create_stocks(incorrect_watch_remnants, offer_ids, incorrect_warehouse_id)
        None
    """
    # Уберем то, что не загружено в market
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создать цены товаров магазина.

    Синхронизирует цены часов с оптового магазина с Яндекс маркет.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        offer_ids (list): Список артикулов товаров Яндекс маркет.

    Returns:
        list: Список артикулов товаров с ценами

    Raises:
        KeyError: Если ключи "Код", "Цена" отсутствуют в словаре watch
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """Загрузить остакти товаров на сервер Яндекс маркет.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        campaign_id (str): Идентификатор магазина в кабинете.
        market_token (str): Токен, необходимый для доступа к API.

    Returns:
        tuple: Список артикулов товаров с информацией об остатках.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    """Загрузить остакти товаров на сервер Яндкес маркета.

    Args:
        watch_remnants (dict): Словарь с информацией о часах.
        campaign_id (str): Идентификатор магазина в кабинете.
        market_token (str): Токен, необходимый для доступа к API.
        warehouse_id (int): Идентификатор склада на Яндекс Маркет.

    Returns:
        tuple: Список артикулов товаров с информацией об остатках.
    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
