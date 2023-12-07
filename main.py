import logging
from datetime import datetime
import asyncio
import json
import os

import openpyxl
from aiogram import Bot, Dispatcher, types
from aiogram.filters.command import Command
from dotenv import load_dotenv
import pandas as pd
from aiogoogle.auth.creds import ServiceAccountCreds
from aiogoogle import Aiogoogle

load_dotenv()
token = os.getenv("TOKEN")
file_id = os.getenv("FILE_ID")

SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = json.load(open('Credentials/service_creds.json'))

logging.basicConfig(level=logging.INFO)
bot = Bot(token=token)
dp = Dispatcher()


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    with open("users.txt", "a+") as f:
        f.seek(0)
        users = f.readlines()
        if str(message.chat.id) + "\n" not in users:
            f.write(str(message.chat.id) + '\n')
        else:
            print(f"{str(message.chat.id)} is already in list")


# @dp.message(Command("alive"))
# async def test(message: types.Message):
#     print("alive")


async def download_sheet(name="commit"):
    os.makedirs("ComparingSheets", exist_ok=True)

    creds = ServiceAccountCreds(scopes=SCOPES, **SERVICE_ACCOUNT_FILE)

    async with Aiogoogle(service_account_creds=creds) as aiog:
        gdrive = await aiog.discover('drive', 'v3')

        file_path = f'ComparingSheets/{name}.xlsx'

        file_response = await aiog.as_service_account(
            gdrive.files.export(fileId=file_id,
                                mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        )

        with open(file_path, 'wb') as file:
            file.write(file_response)

        print(f"[{datetime.now().strftime('%H:%M:%S')}] Synced!")


async def compare():
    try:
        df1 = pd.read_excel('ComparingSheets/initial.xlsx')
        df1['Original_Index'] = df1.index + 2
        df1['ССЫЛКА НА ВИДЕОАРХИВ'] = df1['ССЫЛКА НА ВИДЕОАРХИВ'].astype(str)

        df2 = pd.read_excel('ComparingSheets/commit.xlsx')
        df2['Original_Index'] = df2.index + 2
        df2['ССЫЛКА НА ВИДЕОАРХИВ'] = df2['ССЫЛКА НА ВИДЕОАРХИВ'].astype(str)

        merged_df = pd.merge(df1, df2, how='outer', indicator=True)
        differences = merged_df[merged_df['_merge'] != 'both']

        # Фильтрация только тех строк, которые есть во втором файле
        differences_in_second_file = differences[differences['_merge'] == 'right_only']
        # todo Раскоментить для получения заголовков
        # print(differences_in_second_file)

        column_index = df2.columns.get_loc('Unnamed: 2') + 1

        different_indices = differences_in_second_file['Original_Index'].tolist()

        different_names = differences_in_second_file['Название видео']

        different_tags = differences_in_second_file['Теги'].tolist()

        hyperlinks_differences = extract_hyperlinks('ComparingSheets/commit.xlsx', column_index, different_indices)
        links_dif = list(hyperlinks_differences.values())

        updates = []

        # Можно добавить в табличку булевый столбик статус и высылать результаты если столбик статус чем-то заполнен
        for index, name, link, tag in zip(different_indices, different_names, links_dif, different_tags):
            if not pd.isna(index) and not pd.isna(name) and not pd.isna(link) and not pd.isna(tag):
                if index in df1['Original_Index'].tolist():
                    text = f"Обновление таблицы:\n Строка: {index}\n Название: {name}\n Ссылка: {link}\n Теги: {tag}\n"
                    print(text)
                    updates.append(text + "\n\n")
                else:
                    text = f"Обновление таблицы:\n Строка: {index}\n Название: {name}\n Ссылка: {link}\n Теги: {tag}\n"
                    print(text)
                    updates.append(text + "\n\n")
            else:
                print(f"Skipping incomplete entry at index {index}")

        os.remove('ComparingSheets/initial.xlsx')
        os.rename('ComparingSheets/commit.xlsx', 'ComparingSheets/initial.xlsx')
        return updates
    except IOError:
        await download_sheet("initial")
    except Exception as e:
        print("Error in compare:", e)


def extract_hyperlinks(file_path, column_index, indices):
    workbook = openpyxl.load_workbook(file_path, data_only=False)
    sheet = workbook.active
    hyperlinks = {}

    for index in indices:
        cell = sheet.cell(row=index, column=column_index)
        if cell.hyperlink:
            hyperlinks[index] = cell.hyperlink.target

    return hyperlinks


async def procedures():
    while True:
        try:
            await download_sheet()
            msg = await compare()
            text = ''
            if len(msg) != 0:
                for update in msg:
                    text += update
                with open("users.txt", "r") as f:
                    while user := f.readline():
                        await bot.send_message(chat_id=user, text=text, disable_web_page_preview=True)
        except Exception as e:
            print("Error in procedures:", e)
        await asyncio.sleep(1)


async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    procedures_task = asyncio.create_task(procedures())
    polling_task = asyncio.create_task(dp.start_polling(bot))
    await procedures_task


if __name__ == "__main__":
    asyncio.run(main())
