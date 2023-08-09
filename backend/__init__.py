import asyncio
import logging

from backend.helpers import WeatherParsing
from backend.helpers import parse_and_save_weather_for_all_cities

logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w")


async def main():
    """Запускаем парсер каждые 600 секунд."""
    print("Parse")
    while True:
        await asyncio.sleep(600)
        await parse_and_save_weather_for_all_cities()


asyncio.create_task(main())

