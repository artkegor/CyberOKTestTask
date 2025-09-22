Для запуска сканера выполните следующие шаги:

1. Убедитесь, что у вас установлен Docker.
2. Убедитесь, что у вас установлен Python 3.8 или выше.
3. Склонируйте репозиторий с кодом сканера:

```bash
git clone <repository_url>
cd <repository_directory>
```

4. Создайте и активируйте виртуальное окружение для Python:

```bash
python -m venv venv
source venv/bin/activate  # Для Linux/Mac
venv\Scripts\activate  # Для Windows
```

5. Создайте файл `.env` в корневой директории проекта и добавьте необходимые переменные окружения (пример в
   .env.example).
6. Соберите файл `docker-compose.yml` с необходимой конфигурацией для ClickHouse:

```bash
docker compose up -d
```

7. Проверьте, что контейнер ClickHouse работает корректно:

```bash
docker ps
```

8. После запуска контейнера, создайте необходимые таблицы в ClickHouse, выполнив SQL-скрипт `create_tables.sql` внутри
   контейнера:

```bash
docker exec -i clickhouse clickhouse-client < create_tables.sql
```

9. Установите необходимые Python-библиотеки для работы со сканером:

```bash
pip install -r requirements.txt
```

10. Запустите сканер, указав необходимые параметры:

```bash
python main.py <путь_к_файлу> [--extended | --short] [--batch_size N]
```
11. Логи сканера будут сохраняться в файл `app.log` в корневой директории проекта.
12. По завершении работы сканера в БД ClickHouse будут добавлены записи с результатами сканирования. Это две таблицы - 
   `scan_results` (сырые строки) и `products` (сканы products из файла).