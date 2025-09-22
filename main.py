import sys
import time
import asyncio
from typing import List, Tuple, Dict, Any, Optional

import aiofiles
import orjson as json
from clickhouse_connect import get_async_client
from clickhouse_connect.driver.asyncclient import AsyncClient

from logging_config import setup_logging
from config import (
    CLICKHOUSE_HOST,
    CLICKHOUSE_PORT,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_DB,
)


# --- Парсинг продуктов ---
async def parse_products(scan_id: str, products_data: Optional[List[Dict[str, Any]]]) -> List[Tuple]:
    """
    Подготавливает список записей для вставки в таблицу `scans.products`.

    :param scan_id: Уникальный идентификатор скана (ip_port_timestamp).
    :param products_data: Список словарей с данными о продуктах.
    :return: Список кортежей для вставки в ClickHouse.
    """
    return [
        (
            scan_id,
            product.get("probe", ""),
            product.get("service", ""),
            product.get("regex", ""),
            int(product.get("softmatch", 0)),
            product.get("vendorproductname"),
            product.get("info"),
            product.get("os"),
            product.get("devicetype"),
            product.get("hostname"),
            product.get("cpe") or [],
        )
        for product in (products_data or [])
    ]


# --- Парсинг строки из файла ---
async def parse_line(line: str) -> Tuple[List[Any], List[Dict[str, Any]]]:
    """
    Разбирает строку JSON и подготавливает данные для вставки в таблицу `scans.scan_results`.

    :param line: Одна строка JSON из файла.
    :return: Кортеж: (данные для таблицы `scan_results`, список продуктов).
    """
    data: Dict[str, Any] = json.loads(line)

    ssl_tls: int = int(data.get("ssl/tls", False))
    products_data: List[Dict[str, Any]] = data.get("products") or []
    products_count: int = len(products_data)
    product_services: List[str] = list(
        {p.get("service") for p in products_data if p.get("service")}
    )

    row: List[Any] = [
        f"{data.get('ip')}_{data.get('port')}_{data.get('timestamp')}",  # scan_id
        data.get("ip", ""),
        data.get("port", 0),
        data.get("protocol", ""),
        ssl_tls,
        data.get("used_probes") or {},
        data.get("scan_tries", 0),
        data.get("sended_probes", 0),
        data.get("banners") or {},
        data.get("timestamp", 0),
        data.get("total_time_spent", ""),
        data.get("hex_banners") or {},
        data.get("banners_hashes") or {},
        products_count,
        product_services,
    ]
    return row, products_data


# --- Вставка пачки данных в ClickHouse ---
async def insert_batch(
        client: AsyncClient,
        scan_batch: List[List[Any]],
        products_batch: List[Tuple],
) -> None:
    """
    Вставляет пачку данных в ClickHouse.

    :param client: Асинхронный клиент ClickHouse.
    :param scan_batch: Список записей для таблицы `scan_results`.
    :param products_batch: Список записей для таблицы `products`.
    """
    try:
        if scan_batch:
            await client.insert(
                "scans.scan_results",
                scan_batch,
                settings={"async_insert": 1, "wait_for_async_insert": 0},
            )
        if products_batch:
            await client.insert(
                "scans.products",
                products_batch,
                settings={"async_insert": 1, "wait_for_async_insert": 0},
            )
    except Exception as e:
        logger.error(f"Ошибка вставки в ClickHouse: {e}")


# --- Основная логика ---
async def main(file_path: str, extended_scan: bool, batch_size: int) -> None:
    """
    Основной процесс обработки файла:
    - Чтение файла построчно
    - Парсинг данных из строки
    - Сбор батчей
    - Вставка данных в ClickHouse

    :param file_path: Путь к входному файлу.
    :param extended_scan: Тип сканирования.
    """
    scan_batch: List[List[Any]] = []
    products_batch: List[Tuple] = []
    processed: int = 0
    start_total = start_chunk = time.time()

    logger.info(f"Начинаем обработку файла: {file_path}")

    async with await get_async_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            database=CLICKHOUSE_DB,
    ) as client:
        async with aiofiles.open(file_path, "r") as f:
            async for line in f:
                line = line.strip()
                if not line:
                    processed += 1
                    continue

                row, products_data = await parse_line(line)
                scan_batch.append(row)
                if extended_scan:
                    products_batch.extend(await parse_products(row[0], products_data))

                processed += 1
                if processed % 100_000 == 0:
                    now = time.time()
                    logger.info(
                        f"Обработано строк: {processed} | "
                        f"Время на последние 100k: {now - start_chunk:.2f}s | "
                        f"Всего: {now - start_total:.2f}s"
                    )
                    start_chunk = now

                if len(scan_batch) >= batch_size:
                    await insert_batch(client, scan_batch, products_batch)
                    scan_batch.clear()
                    products_batch.clear()

        # Финальная вставка (если остались данные)
        if scan_batch or products_batch:
            await insert_batch(client, scan_batch, products_batch)

    logger.info(
        f"Готово! Всего обработано строк: {processed} | "
        f"Общее время: {time.time() - start_total:.2f}s"
    )


# --- Точка входа ---
if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(
            "Использование: python main.py <путь_к_файлу> [--extended | --short] [--batch_size N]\n\n"
            "--extended   - расширенное сканирование с продуктами,\n"
            "--short      - быстрое сканирование (сырые строки),\n"
            "--batch_size - размер пачки для вставки (по умолчанию 10000)."
        )
        sys.exit(1)

    file_path = sys.argv[1]

    # Значения по умолчанию
    extended = True
    batch_size = 10_000

    # Парсим дополнительные аргументы
    args = sys.argv[2:]
    i = 0
    while i < len(args):
        if args[i] == "--extended":
            extended = True
            i += 1
        elif args[i] == "--short":
            extended = False
            i += 1
        elif args[i] == "--batch_size":
            if i + 1 < len(args):
                try:
                    batch_size = int(args[i + 1])
                    i += 2
                except ValueError:
                    print("Ошибка: batch_size должен быть целым числом.")
                    sys.exit(1)
            else:
                print("Ошибка: отсутствует значение для batch_size.")
                sys.exit(1)
        else:
            print(f"Неверный аргумент: {args[i]}. Используйте --extended, --short или --batch_size")
            sys.exit(1)

    # --- Логирование ---
    logger = setup_logging("app.log", level="INFO")

    # --- Запуск ---
    asyncio.run(main(file_path, extended, batch_size))
