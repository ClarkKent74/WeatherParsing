import requests
from bs4 import BeautifulSoup as bs
import logging
from backend.schemas import DefaultResponse
import os
import token
import aiohttp
import asyncio
from backend.models import SessionManager, City, Weather
from sqlalchemy import select, delete
from datetime import datetime


TOKEN = os.environ['TOKEN_WEATHER']


class WeatherParsing:

    def __init__(self, city):
        self.city = city

    async def get_city(self):
        try:
            logging.info("Парсим город")
            async with aiohttp.ClientSession() as session:
                url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}" \
                      f"&appid={TOKEN}&units=metric"
                async with session.get(url) as response:
                    data = await response.json()
                    city = data['name']
                    return city
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return "none"

    async def get_cur_weather(self):
        try:
            logging.info("Парсим температуру")
            async with aiohttp.ClientSession() as session:
                url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}" \
                      f"&appid={TOKEN}&units=metric"
                async with session.get(url) as response:
                    data = await response.json()
                    cur_weather = int(data['main']['temp'])
                    #print(f'Температура {cur_weather}\n')
                    return cur_weather
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return {"error": str(e)}

    async def get_humidity(self):
        try:
            logging.info("Парсим влажность")
            async with aiohttp.ClientSession() as session:
                url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}" \
                      f"&appid={TOKEN}&units=metric"
                async with session.get(url) as response:
                    data = await response.json()
                    humidity = data['main']['humidity']
                    #print(f'Влажность: {humidity}\n')
                    return humidity
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return {"error": str(e)}

    async def get_pressure(self):
        try:
            logging.info("Парсим давление")
            async with aiohttp.ClientSession() as session:
                url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}" \
                      f"&appid={TOKEN}&units=metric"
                async with session.get(url) as response:
                    data = await response.json()
                    pressure = data['main']['pressure']
                    #print(f'Давление: {pressure}\n')
                    return pressure
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return {"error": str(e)}

    async def get_feeling(self):
        try:
            logging.info("Парсим ощущения")
            async with aiohttp.ClientSession() as session:
                url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}" \
                      f"&appid={TOKEN}&units=metric"
                async with session.get(url) as response:
                    data = await response.json()
                    feeling = int(data['main']['feels_like'])
                    #print(f'Ощущается как: {feeling}\n')
                    return feeling
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return {"error": str(e)}

    async def get_wind(self):
        try:
            logging.info("Парсим скорость ветра")
            async with aiohttp.ClientSession() as session:
                url = f"https://api.openweathermap.org/data/2.5/weather?q={self.city}" \
                      f"&appid={TOKEN}&units=metric"
                async with session.get(url) as response:
                    data = await response.json()
                    wind = data['wind']['speed']
                    #print(f'Ветер: {wind} м\с\n')
                    return wind
        except Exception as e:
            logging.error("Exception", exc_info=True)
            return {"error": str(e)}


def check_digit(town: str):
    for symb in town:
        if symb.isdigit():
            return True
    return False


async def parse_and_save_weather_for_city(city):
    print(city)
    try:
        async with SessionManager() as session:
            weather_parser = WeatherParsing(city.city)

            weather = Weather(
                temperature=str(await weather_parser.get_cur_weather()),
                pressure=str(await weather_parser.get_pressure()),
                humidity=str(await weather_parser.get_humidity()),
                wind=str(await weather_parser.get_wind()),
                feeling=str(await weather_parser.get_feeling()),
                date=datetime.now(),
            )
            #city.weathers.append(weather)
            print(weather)
            session.add(weather)
            logging.info("Парсим погоду для города %s", city.city)
            await session.commit()
    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message=str(e), payload=None)
        return response


async def parse_and_save_weather_for_all_cities():
    async with SessionManager() as session:
        cities = (await session.execute(select(City))).all()
        logging.info("Находим города в базе")
        tasks = [parse_and_save_weather_for_city(city[0]) for city in cities]
        await asyncio.gather(*tasks)


