import asyncio
import datetime
from aiohttp import ClientSession
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, JSON, Text, String


DSN = 'postgresql+asyncpg://XXXXXXX:XXXXXXXX127.0.0.1:5431/ntl_asyncio'
engine = create_async_engine(DSN)
Base = declarative_base()
Session = sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False
    )


class Person(Base):
    __tablename__ = 'persons'
    id = Column(Integer, primary_key=True)
    birth_year = Column(String(50), nullable=True)
    eye_color = Column(String(50), nullable=True)
    films = Column(Text, nullable=True)
    gender = Column(String(50), nullable=True)
    hair_color = Column(String(50), nullable=True)
    height = Column(String(50), nullable=True)
    homeworld = Column(Text, nullable=True)
    mass = Column(String(50), nullable=True)
    name = Column(String(100), nullable=True)
    skin_color = Column(String(50), nullable=True)
    species = Column(Text, nullable=True)
    starships = Column(Text, nullable=True)
    vehicles = Column(Text, nullable=True)


FINAL_HERO_ITEM = 0

async def get_person_data(person_id: int, session: ClientSession):
    async with session.get(
            f'https://swapi.dev/api/people/{person_id}') as response:
        if response.status == 404:
            print("404", person_id)
            async with session.get(
                    f'https://swapi.dev/api/people/{person_id+1}') as res:
                if res.status == 404:
                    global FINAL_HERO_ITEM
                    FINAL_HERO_ITEM = person_id
                    person_data = "Not found"
                else:
                    person_data = "Not found"
        else:
            person_data = await response.json()
            print(f'Person {person_id} was copied from https')
    return person_data


async def chunk_10pos(async_iter):
    buffer = []
    while True:
        try:
            item = await async_iter.__anext__()
        except StopAsyncIteration:
            break
        buffer.append(item)
        if len(buffer) == 10:
            yield buffer
            buffer = []


async def insert_persons(chunk):
    async with Session() as session:
        for position in chunk:
            if position != 'Not found':
                session.add_all([Person(
                    birth_year='name',
                    eye_color='eye_color',
                    films='films',
                    gender='gender',
                    hair_color='hair_color',
                    height='height',
                    homeworld='homeworld',
                    mass='mass',
                    name='name',
                    skin_color='skin_color',
                    species='species',
                    starships='starships',
                    vehicles='vehicles'
                    ) for item in chunk])
                await session.commit()
            else:
                print('Not data')


async def get_all_persons_data():
    async with ClientSession() as session:
        cor = []
        id = 1
        while FINAL_HERO_ITEM == 0:
            cor.append(get_person_data(person_id=id, session=session))
            id += 1
            if len(cor) == 10:
                result = await asyncio.gather(*cor)
                for item in result:
                    yield item
                cor = []


async def add_to_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.commit()

    async for chunk in chunk_10pos(get_all_persons_data()):
        print(f"данные о 10 персонах перед загрузкой в БД: {chunk}")
        asyncio.create_task(insert_persons(chunk))

    tasks = set(asyncio.all_tasks()) - {asyncio.current_task()}
    for task in tasks:
        await task


"""for Windows"""
# asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

app_start = datetime.datetime.now()
asyncio.run(add_to_db())
print(f'App time work: {datetime.datetime.now() - app_start}')