import logging
from datetime import datetime

from sqlalchemy import select, delete

from backend.helpers import WeatherParsing, check_digit
from backend.models import (
    City,
    Weather,
    CityWeather
)
from backend.models import SessionManager
from backend.schemas import DefaultResponse, PressureResponse, TemperatureResponse, AllWeatherResponse, AverageResponse


async def view_all(id: int) -> DefaultResponse:
    """
    Выводим все поля погоды.
    :param id: вводим id города
    :return: DefaultResponse
    """
    logging.info("Находим город в базе")
    try:
        async with SessionManager() as session:
            logging.info("Находим город в базе")

            result = []

            weathers = (await session.execute(select(Weather)
                                              .join(CityWeather, Weather.id == CityWeather.weather_id)
                                              .join(City, CityWeather.city_id == City.id).filter(City.id == id)
                                              .order_by(Weather.date.desc()).limit(1))).all()
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
                response = DefaultResponse(error=False, message="Ok",
                                           payload=AllWeatherResponse(weather=result))
            else:
                response = DefaultResponse(error=True, message="введено некорректное значение "
                                                               "или города нет в базе", payload=None)
    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение "
                                                       "или города нет в базе", payload=None)
    return response


async def view_pressure(id: int) -> DefaultResponse:
    """
    Выводим давление.
    :param id: вводим id города.
    :return: DefaultResponse
    """
    try:
        async with SessionManager() as session:
            logging.info("Находим город в базе")
            result = []
            weathers = (await session.execute(select(Weather)
                                              .join(CityWeather, Weather.id == CityWeather.weather_id)
                                              .join(City, CityWeather.city_id == City.id).filter(City.id == id)
                                              .order_by(Weather.date.desc()).limit(1))).all()

            for weather in weathers:
                weather_dict = {
                    "pressure": weather[0].pressure,
                }
                logging.info("Получаем информацию о давлении")
                result = list(weather_dict.values())
            if len(result):
                response = DefaultResponse(error=False,
                                           message="OK",
                                           payload=PressureResponse(pressure=result, id=id, date=weather[0].date))
            else:
                response = DefaultResponse(error=True, message="введено некорректное значение "
                                                               "или города нет в базе", payload=None)

    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение "
                                                       "или города нет в базе", payload=None)
        print(e)
    return response


async def view_temp(id: int) -> DefaultResponse:
    """
    Выводим температуру.
    :param id: вводим id города
    :return: DefaultResponse
    """
    try:
        async with SessionManager() as session:
            logging.info("Находим город в базе")
            result = []
            weathers = (await session.execute(select(Weather)
                                              .join(CityWeather, Weather.id == CityWeather.weather_id)
                                              .join(City, CityWeather.city_id == City.id).filter(City.id == id)
                                              .order_by(Weather.date.desc()).limit(1))).all()
            for weather in weathers:
                weather_dict = {
                    "temperature": weather[0].temperature,
                }
                logging.info("Получаем температуру")
                result = list(weather_dict.values())
            if len(result):
                response = DefaultResponse(error=False,
                                           message="OK",
                                           payload=TemperatureResponse(temperature=result, id=id, date=weather[0].date))
            else:
                response = DefaultResponse(error=True, message="введено некорректное значение "
                                                               "или города нет в базе", payload=None)
    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение "
                                                       "или города нет в базе", payload=None)
    return response


async def add(town: str) -> DefaultResponse:
    """
    Добавляем город на мониторинг.
    :param town: вводим название города на английском
    :return: DefaultResponse
    """
    try:
        async with SessionManager() as session:
            if check_digit(town):
                response = DefaultResponse(error=True, message="Город не может содержать цифры", payload=None)
            else:
                weather_parser = WeatherParsing(town)
                weather_data = await weather_parser.parse_weather_data()
                city_name = weather_data["city"]
                city = (await session.execute(select(City).filter(City.city == city_name))).first()
                logging.info("Находим город в базе")
                if not city:
                    city_entity = City(city=city_name)
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


async def viewing_statistics(id: int, limit: int, offset: int) -> DefaultResponse:
    """
    Выводим всю существующую статистику.
    :param id: вводим id города
    :param limit: вводим количество записей, которое хотим получить
    :param offset: вводим отступ
    :return: DefaultResponse
    """
    try:
        async with SessionManager() as session:
            result = []
            weathers = (await session.execute(select(Weather)
                                              .join(CityWeather, Weather.id == CityWeather.weather_id)
                                              .join(City, CityWeather.city_id == City.id).filter(City.id == id)
                                              .order_by(Weather.date.desc()).limit(limit).offset(offset))).all()

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
                response = DefaultResponse(error=False, message="Ok", payload=AllWeatherResponse(weather=result))
            else:
                response = DefaultResponse(error=True, message="введено некорректное значение"
                                                               "или город не найден", payload=None)

    except Exception as e:
        # await session.rollback()
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="введено некорректное значение"
                                                       "или город не найден", payload=None)
        print(e)
    return response


async def remove(id: int) -> DefaultResponse:
    """
    Удаляем город с мониторинга.
    :param id: вводим id города
    :return: DefaultResponse
    """
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
        response = DefaultResponse(error=True, message="Введено некорректное значение или города нет в базе",
                                   payload=None)
        print(e)
    return response


async def average_temp(id: int, data: str) -> DefaultResponse:
    """
    Получаем среднюю температуру.
    :param id: вводим id города
    :param data: вводим дату в формате 2023-08-08
    :return: DefaultResponse
    """
    try:
        async with SessionManager() as session:
            city = (await session.execute(select(City).filter(City.id == id))).scalars().one()

            if not city:
                response = DefaultResponse(error=True, message="Города нет в базе или "
                                                               "введено некорректное значение", payload=None)
            else:

                parsed_date = datetime.strptime(data, "%Y-%m-%d").date()

                weather_ids = (await session.execute(
                    select(CityWeather.weather_id)
                    .join(Weather, Weather.id == CityWeather.weather_id)
                    .filter(CityWeather.city_id == id, Weather.date == parsed_date)
                )).all()

                if not weather_ids:
                    response = DefaultResponse(error=True, message="Данных о погоде для указанной даты нет в базе",
                                               payload=None)
                else:

                    weather_ids = [id[0] for id in weather_ids]

                    temperatures = (await session.execute(
                        select(Weather.temperature)
                        .filter(Weather.id.in_(weather_ids))
                    )).all()

                    temperatures = [float(temp[0]) for temp in temperatures]

                    average_temperature = round(sum(temperatures) / len(temperatures), 2)
                    logging.info("Получаем среднюю температуру")

                    response = DefaultResponse(error=False,
                                               message="Ok",
                                               payload=AverageResponse(temperature=average_temperature,
                                                                       id=city.id, date=data))
    except Exception as e:
        logging.error("Exception", exc_info=True)
        response = DefaultResponse(error=True, message="Введено некорректное значение", payload=None)
        print(e)
    return response
