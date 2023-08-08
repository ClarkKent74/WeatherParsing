from fastapi import FastAPI
from DataBase import view_all, add, viewing_statistics, view_pressure, view_temp, remove, average_temp
from backend.schemas import DefaultResponse, PressureResponse, TemperatureResponse
import asyncio
from backend.helpers import parse_and_save_weather_for_all_cities

app = FastAPI(
    title='Weather Parsing'
)


@app.get("/weather/view_weather")
async def view_all_weather(id: int) -> DefaultResponse:
    """
    Получаем всю информацию о погоде в заданном городе.
    Вводим id города.
    """
    response = await view_all(id)

    return response


@app.post("/weather/add")
async def add_on_view_statistics(city: str) -> DefaultResponse:
    """
    Добавляем город для получения дальнейшей статистики.
    Формат города: Moscow, Saratov, Chelyabinsk и т.д.
    """
    response = await add(city)
    return response

    # try:
    #     success = add(city)
    #     response = DefaultResponse(error=False, message="Ok", payload=success)
    # except Exception as e:
    #     response = DefaultResponse(
    #         error=True, message=e, payload=None)
    # return response


@app.get("/weather/statistics")
async def view_statistics(id: int, limit: int = 10, offset: int = 0) -> DefaultResponse:
    """
    Просматриваем статистику в заданном городе.
    Вводим id города.
    """
    response = await viewing_statistics(id, limit, offset)
    return response


@app.get("/weather/pressure")
async def get_pressure(id: int) -> DefaultResponse:
    """
    Получаем давление в заданном городе в мм рт ст.
    Вводим id города.
    """
    response = await view_pressure(id)

    return response


@app.get("/weather/temperature")
async def get_temperature(id: int) -> DefaultResponse:
    """
    Получаем температуру в заданном городе в градусах Цельсия.
    Вводим id города.
    """
    response = await view_temp(id)
    return response


@app.delete("/weather/delete")
async def remove_city(id: int) -> DefaultResponse:
    """
    Удаляем город из просмотра статистики.
    Вводим id города.
    """
    response = await remove(id)
    return response


@app.get("/weather/average_temp")
async def get_average_temp(id: int, data: str = "2023-08-08") -> DefaultResponse:
    """
    Получаем среднюю температуру в городе.
    Вводим id города.
    Формат даты: 2023-07-30
    """
    response = await average_temp(id, data)

    return response




