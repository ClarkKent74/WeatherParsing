import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

import aiohttp
from sqlalchemy import select

from backend.models import SessionManager, City, Weather, CityWeather
from backend.schemas import DefaultResponse

TOKEN = os.environ['TOKEN_WEATHER']


class WeatherParsing:
    """Класс для парсинга погоды."""
    def __init__(self, city: str):
        self.city = city
        self.url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}&appid={TOKEN}&units=metric"

    async def get_data(self):
        """Получаем .json файл с погодой."""
        try:
            logging.info("Парсим город")
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url) as response:
                    data = await response.json()
                    return data
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return "none"

    async def parse_weather_data(self) -> Optional[dict]:
        """Берем из файла погоду."""
        try:
            data = await self.get_data()
            if data:
                return {
                    "city": str(data['name']),
                    "temperature": str(int(data['main']['temp'])),
                    "pressure": str(data['main']['pressure']),
                    "humidity": str(data['main']['humidity']),
                    "wind": str(data['wind']['speed']),
                    "feeling": str(int(data['main']['feels_like'])),
                }
            else:
                return None
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return {"error": str(e)}


def check_digit(town: str) -> bool:
    """
    Проверка на наличие цифр в городе.
    :param town: название города на английском.
    :return: bool
    """
    for symb in town:
        if symb.isdigit():
            return True
    return False


async def parse_and_save_weather_for_city(city: str):
    """
    Парсим погоду и записываем в базу данных.
    :param city: получаем название города на английском
    :return: DefaultResponse в случае ошибки
    """
    print(city)
    try:
        async with SessionManager() as session:
            weather_parser = WeatherParsing(city.city)
            weather_data = await weather_parser.parse_weather_data()

            if weather_data:
                weather = Weather(
                    temperature=weather_data["temperature"],
                    pressure=weather_data["pressure"],
                    humidity=weather_data["humidity"],
                    wind=weather_data["wind"],
                    feeling=weather_data["feeling"],
                    date=datetime.now().date(),
                )
            # city.weathers.append(weather)
            print(weather)
            session.add(weather)
            logging.info("Парсим погоду для города %s", city.city)
            await session.flush()
            await session.refresh(weather)
            w_id = (await session.execute(select(Weather.id).order_by(Weather.id.desc()))).first()
            print(w_id)
            city_weather = CityWeather(city_id=city.id, weather_id=w_id[0])
            print(city.id)
            session.add(city_weather)
            await session.commit()

    except Exception as e:
        print(e)
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message=str(e), payload=None)
        return response


async def parse_and_save_weather_for_all_cities():
    """Запускаем парсинг для всех городов из базы."""
    async with SessionManager() as session:
        cities = (await session.execute(select(City))).all()
        logging.info("Находим города в базе")
        tasks = [parse_and_save_weather_for_city(city[0]) for city in cities]
        await asyncio.gather(*tasks)
