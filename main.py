from fastapi import FastAPI

from DataBase import view_all, add, viewing_statistics, view_pressure, view_temp, remove, average_temp, view_cities
from backend.schemas import DefaultResponse

app = FastAPI(
    title='Weather Parsing'
)


@app.get("/weather/view_weather", response_model=DefaultResponse)
async def view_all_weather(id: int) -> DefaultResponse:
    """
    Получаем всю информацию о погоде в заданном городе.
    :param id: вводим id города
    :return: DefaultResponse
    """
    response = await view_all(id)
    return response


@app.post("/weather/add", response_model=DefaultResponse)
async def add_on_view_statistics(city: str) -> DefaultResponse:
    """
    Добавляем город для получения дальнейшей статистики.
    :param city: Формат города: Moscow, Saratov, Chelyabinsk и т.д.
    :return: DefaultResponse
    """
    response = await add(city)
    return response


@app.get("/weather/statistics", response_model=DefaultResponse)
async def view_statistics(id: int, limit: int = 10, offset: int = 0) -> DefaultResponse:
    """
    Просматриваем статистику в заданном городе.
    :param offset: вводим отступ
    :param limit: вводим количество, которое хотим получить
    :param id: вводим id города
    :return: DefaultResponse
    """
    response = await viewing_statistics(id, limit, offset)
    return response


@app.get("/weather/pressure", response_model=DefaultResponse)
async def get_pressure(id: int) -> DefaultResponse:
    """
    Получаем давление в заданном городе в мм рт ст.
    :param id: вводим id города
    :return: DefaultResponse
    """
    response = await view_pressure(id)
    return response


@app.get("/weather/temperature", response_model=DefaultResponse)
async def get_temperature(id: int) -> DefaultResponse:
    """
    Получаем температуру в заданном городе в градусах Цельсия.
    :param id: вводим id города
    :return: DefaultResponse
    """
    response = await view_temp(id)
    return response


@app.delete("/weather/delete", response_model=DefaultResponse)
async def remove_city(id: int) -> DefaultResponse:
    """
    Удаляем город из просмотра статистики.
    :param id: вводим id города
    :return: DefaultResponse
    """
    response = await remove(id)
    return response


@app.get("/weather/average_temp", response_model=DefaultResponse)
async def get_average_temp(id: int, data: str = "2023-08-08") -> DefaultResponse:
    """
    Получаем среднюю температуру в городе.
    :param id: Вводим id города.
    :param data: Формат даты: 2023-07-30
    :return: DefaultResponse
    """
    response = await average_temp(id, data)
    return response


@app.get("/weather/cities_list", response_model=DefaultResponse)
async def get_cities_list() -> DefaultResponse:
    """
    Выводим список городов на парсинге.
    :return: DefaultResponse
    """
    response = await view_cities()
    return response




