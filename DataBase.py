from datetime import datetime, date
from backend.helpers import WeatherParsing, check_digit
import schedule
import time
import psycopg2
import logging
import asyncio

from sqlalchemy import select, delete
from backend.models import Base, SessionManager
from backend.schemas import DefaultResponse
from backend.models import (
    City,
    Weather,
    CityWeather
)


async def view_all(id: int):
    logging.info("Находим город в базе")
    try:
        async with SessionManager() as session:
            cities_id = (await session.execute(select(City.id).filter(City.id == id))).scalars().one()
            logging.info("Находим город в базе")
            if not cities_id:
                response = DefaultResponse(error=True, message="Город не найден или "
                                                               "введено некорректное значение", payload=None)

            result = []

            weathers_id = (await session.execute(select(CityWeather.weather_id).
                                                 filter(CityWeather.city_id == cities_id))).all()

            result_id = []
            for weather_id in weathers_id:
                result_id.append(weather_id[0])

            weathers = (await session.execute(select(Weather).filter(Weather.id.in_(result_id)))).all()
            for weather in weathers:
                weather_dict = {
                    "id": weather[0].id,
                    "temperature": weather[0].temperature,
                    "pressure": weather[0].pressure,
                    "humidity": weather[0].humidity,
                    "wind": weather[0].wind,
                    "feeling": weather[0].feeling,
                    "date": weather[0].date.strftime("%Y-%m-%d"),
                }
                logging.info("Получаем полную статистику о погоде")
                result.append(weather_dict)
            if len(result):
                response = DefaultResponse(error=False, message="Ok", payload=result[-1])
    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение "
                                                       "или города нет в базе", payload=None)
    return response


async def view_pressure(id: int):
    try:
        async with SessionManager() as session:
            logging.info("Находим город в базе")
            cities_id = (await session.execute(select(City.id).filter(City.id == id))).scalars().one()
            if not cities_id:
                response = DefaultResponse(error=True, message="Город не найден или "
                                                               "введено некорректное значение", payload=None)
            result = []

            weathers_id = (await session.execute(
                select(CityWeather.weather_id).filter(CityWeather.city_id == cities_id))).all()

            result_id = []
            for weather_id in weathers_id:
                result_id.append(weather_id[0])

            weathers = (await session.execute(select(Weather).filter(Weather.id.in_(result_id)))).all()
            for weather in weathers:
                weather_dict = {
                    "pressure": weather[0].pressure,
                }
                logging.info("Получаем информацию о давлении")
                result.append(weather_dict)
            if len(result):
                response = DefaultResponse(error=False, message="Ok", payload=result[-1])

    except Exception as e:
        #db.rollback()
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение "
                                                       "или города нет в базе", payload=None)
        print(e)
    return response


async def view_temp(id: int):
    try:
        async with SessionManager() as session:
            cities_id = (await session.execute(select(City.id).filter(City.id == id))).scalars().one()
            logging.info("Находим город в базе")
            if not cities_id:
                response = DefaultResponse(error=True, message="Города нет в базе или "
                                                                "введено некорректное значение", payload=None)
            result = []

            weathers_id = (await session.execute(
                select(CityWeather.weather_id).filter(CityWeather.city_id == cities_id))).all()

            result_id = []
            for weather_id in weathers_id:
                result_id.append(weather_id[0])

            weathers = (await session.execute(select(Weather).filter(Weather.id.in_(result_id)))).all()
            for weather in weathers:
                weather_dict = {
                    "temperature": weather[0].temperature,
                }
                logging.info("Получаем температуру")
                result.append(weather_dict)
            if len(result):
                response = DefaultResponse(error=False, message="Ok", payload=result[-1])
    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение "
                                                       "или города нет в базе", payload=None)
    return response


async def add(town: str):
    try:
        async with SessionManager() as session:
            if check_digit(town):
                response = DefaultResponse(error=True, message="Город не может содержать цифры", payload=None)
            else:
                weather_parser = WeatherParsing(town)
                city_name = await weather_parser.get_city()
                city = (await session.execute(select(City).filter(City.city == city_name))).first()
                logging.info("Находим город в базе")
                if not city:
                    weather = Weather(
                        temperature=str(await weather_parser.get_cur_weather()),
                        pressure=str(await weather_parser.get_pressure()),
                        humidity=str(await weather_parser.get_humidity()),
                        wind=str(await weather_parser.get_wind()),
                        feeling=str(await weather_parser.get_feeling()),
                        date=datetime.now(),
                    )
                    city_entity = City(city=city_name)
                    city_entity.weathers.append(weather)
                    logging.info("Добавляем город на мониторинг")
                    session.add(city_entity)
                    await session.commit()
                    response = DefaultResponse(error=False, message="Город успешно добавлен", payload=None)
                else:
                    response = DefaultResponse(error=True, message="Город уже существует в базе", payload=None)

    except Exception as err:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение"
                                                       " или город уже существует в базе", payload=None)
        print(err)
    return response


async def viewing_statistics(id: int):
    try:
        async with SessionManager() as session:
            cities_id = (await session.execute(select(City.id).filter(City.id == id))).scalars().one()
            logging.info("Находим город в базе")
            if not cities_id:
                response = DefaultResponse(error=True, message="Город не найден или "
                                                               "введено некорректное значение", payload=None)

            result = []

            weathers_id = (await session.execute(select(CityWeather.weather_id).
                                                 filter(CityWeather.city_id == cities_id))).all()

            result_id = []
            for weather_id in weathers_id:
                result_id.append(weather_id[0])

            weathers = (await session.execute(select(Weather).filter(Weather.id.in_(result_id)))).all()
            for weather in weathers:
                weather_dict = {
                    "id": weather[0].id,
                    "temperature": weather[0].temperature,
                    "pressure": weather[0].pressure,
                    "humidity": weather[0].humidity,
                    "wind": weather[0].wind,
                    "feeling": weather[0].feeling,
                    "date": weather[0].date.strftime("%Y-%m-%d"),
                }
                logging.info("Получаем полную статистику о погоде")
                result.append(weather_dict)
            if len(result):
                response = DefaultResponse(error=False, message="Ok", payload=result)
    except Exception as e:
        #await session.rollback()
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение"
                                                       "или город не найден", payload=None)
        print(e)
    return response


async def remove(id: int):
    try:
        async with SessionManager() as session:


            logging.info("Находим город в базе")

            weathers_id = (await session.execute(select(CityWeather.weather_id).
                                                 filter(CityWeather.city_id == id))).all()
            result = [weather_id[0] for weather_id in weathers_id]
            await session.execute(delete(CityWeather).where(CityWeather.city_id == id))
            await session.execute(delete(Weather).where(Weather.id.in_(result)))
            logging.info("Удаляем город")
            await session.execute(delete(City).where(City.id == id))
            await session.commit()
            response = DefaultResponse(error=False, message="Город успешно удален", payload=None)
    except Exception as e:
        # db.rollback()
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="Введено некорректное значение или города нет в базе", payload=None)
        print(e)
    return response


async def average_temp(id: int, data: str):
    try:
        async with SessionManager() as session:
            cities_id = (await session.execute(select(City.id).filter(City.id == id))).scalars().one()
            logging.info("Находим город в базе")
            if not cities_id:
                response = DefaultResponse(error=True, message="Города нет в базе или "
                                                               "введено некорректное значение", payload=None)
            result = []

            weathers_id = (await session.execute(
                select(CityWeather.weather_id).filter(CityWeather.city_id == cities_id))).all()

            result_id = []
            for id in weathers_id:
                result_id.append(id[0])

            weathers = (await session.execute(select(Weather).filter(Weather.id.in_(result_id)))).all()
            for weather in weathers:
                weather_dict = {
                    "temperature": weather[0].temperature,
                }
                result.append(weather_dict["temperature"])
            sum_temp = 0
            for temp in result:
                sum_temp += int(float(temp))

            average_temperature = round(sum_temp / len(result), 2)
            logging.info("Получаем среднюю температуру")

        response = DefaultResponse(error=False, message="Ok", payload=average_temperature)
    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="Города или даты нет в базе или "
                                                       "введено некорректное значение", payload=None)
    return response




