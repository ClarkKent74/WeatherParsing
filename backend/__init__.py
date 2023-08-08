from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker, Session, relationship
from sqlalchemy import Column, Integer, String, Date
from datetime import datetime, date
import sqlalchemy as sa
import asyncio
from backend.helpers import WeatherParsing
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
import schedule
import time
import psycopg2
from sqlalchemy.orm.exc import NoResultFound
import os
import logging
from backend.helpers import parse_and_save_weather_for_all_cities


logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w")


async def main():
    print("Parse")
    while True:
        await asyncio.sleep(600)
        await parse_and_save_weather_for_all_cities()


asyncio.create_task(main())

