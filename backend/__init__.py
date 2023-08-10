import asyncio
import logging

from backend.helpers import WeatherParsing
from backend.helpers import parse_and_save_weather_for_all_cities


formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)


logging.basicConfig(level=logging.INFO, handlers=[console_handler])


async def main():
    """Запускаем парсер каждые 30 секунд."""
    print("Parse")
    while True:
        await asyncio.sleep(30)
        await parse_and_save_weather_for_all_cities()


asyncio.create_task(main())

