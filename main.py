import telebot
import json
import bson
import calendar
import pandas as pd
from config import TOKEN
from telebot.async_telebot import AsyncTeleBot
from telebot import types
from datetime import datetime, timedelta
from pymongo import MongoClient

bot = AsyncTeleBot(TOKEN)


async def work_text(j_text):
    with open('sample_collection.bson', 'rb') as f:
        n = ['month', 'day', 'hour']
        freq = ['MS', 'D', 'h']
        data = bson.decode_all(f.read())
        fdown_date = datetime.strptime(j_text['dt_from'], '%Y-%m-%dT%H:%M:%S')
        dt_upto = datetime.strptime(j_text['dt_upto'], '%Y-%m-%dT%H:%M:%S')
        res = pd.date_range(start=fdown_date, end=dt_upto, freq=freq[n.index(j_text["group_type"])]).tolist()
        d = dict((i, [0]) for i in res)
        for i in data:
            if j_text["group_type"] == 'month':
                l = datetime(i['dt'].year, i['dt'].month, 1)
            elif j_text["group_type"] == 'day':
                l = datetime(i['dt'].year, i['dt'].month, i['dt'].day)
            elif j_text["group_type"] == 'hour':
                l = datetime(i['dt'].year, i['dt'].month, i['dt'].day, i['dt'].hour)
            if l in d.keys() and datetime(i['dt'].year, i['dt'].month, i['dt'].day, i['dt'].hour) < dt_upto:
                d[l].append(i['value'])
        f = {}
        for i, j in sorted(d.items()):
            f.setdefault('dataset', []).append(sum(j))
            f.setdefault('labels', []).append(i.isoformat())
    return json.dumps(f)


@bot.message_handler(commands=['start', 'help'])
async def send_welcome(message):
    await bot.send_message(message.chat.id, f'Hi, {message.from_user.first_name}!')


@bot.message_handler(content_types=['text'])
async def func(message):
    try:
        j_text = json.loads(message.text)
        response = await work_text(j_text)
        await bot.send_message(message.chat.id, response)
    except Exception as e:
        await bot.send_message(message.chat.id, 'Невалидный запрос. Пример запроса:\n'
                                                 '{"dt_from": "2022-09-01T00:00:00", '
                                                 '"dt_upto": "2022-12-31T23:59:00", '
                                                 '"group_type": "month"}')


@bot.message_handler(func=lambda m: True)
async def echo_all(message):
    await bot.reply_to(message, message.text)


async def main():
    await bot.remove_webhook()
    await bot.infinity_polling()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
