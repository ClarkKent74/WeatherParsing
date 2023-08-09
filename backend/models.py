import asyncio
import os
from contextlib import asynccontextmanager
from datetime import date
from uuid import uuid4

import sqlalchemy as sa
from asyncpg import Connection
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship

db_user = os.environ['DB_USER']
db_pass = os.environ['DB_PASS']
db_host = os.environ['DB_HOST']
db_port = os.environ['DB_PORT']
db_name = os.environ['DB_NAME']

SQLALCHEMY_DATABASE_URL = f'postgresql+asyncpg://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'
Base = declarative_base()


class CConnection(Connection):
    """Класс для подключения к базе."""
    def _get_unique_id(self, prefix: str) -> str:
        return f'__asyncpg_{prefix}_{uuid4()}__'


engine = create_async_engine(SQLALCHEMY_DATABASE_URL, echo=False, future=True, pool_size=50, connect_args={
    "statement_cache_size": 0, "prepared_statement_cache_size": 0, "connection_class": CConnection, })
Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)


@asynccontextmanager
async def SessionManager() -> Session:
    """Функция для создания сессии для подключения."""
    async with Session() as db:
        try:
            yield db
        except:
            await db.rollback()
            raise
        finally:
            await db.close()


class CityWeather(Base):
    """Класс, связывающий таблицы City и Weather."""
    __tablename__ = "city_weather"
    city_id = Column(Integer, sa.ForeignKey('City.id'), primary_key=True)
    weather_id = Column(Integer, sa.ForeignKey('weather.id'), primary_key=True)
    weathers = relationship('Weather')
    cities = relationship('City')


class City(Base):
    """Класс для таблицы с городами."""
    __tablename__ = "City"
    id = Column(Integer, primary_key=True, index=True)
    city = Column(String)


class Weather(Base):
    """Класс для таблицы с погодой."""
    __tablename__ = "weather"
    id = Column(Integer, primary_key=True, index=True)
    temperature = Column(String)
    pressure = Column(String)
    humidity = Column(String)
    wind = Column(String)
    feeling = Column(String)
    date = Column(Date, default=date.today)


async def init_models():
    """Функция для подключения к базе."""
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


asyncio.create_task(init_models())
