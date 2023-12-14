import logging
from datetime import datetime
import asyncio
import json
import os
import re

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

if not os.path.exists('users.txt'):
    f = open("users.txt", 'w')
    f.write("")
    f.close()


@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    with open("users.txt", "a+") as f:
        f.seek(0)
        users = f.readlines()
        if str(message.chat.id) + "\n" not in users:
            f.write(str(message.chat.id) + '\n')
        else:
            print(f"{str(message.chat.id)} is already in list")


async def download_sheet(name="commit"):
    os.makedirs("Credentials", exist_ok=True)
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
        df1['Country'] = df1['Country'].astype(str)
        df1['Unnamed: 17'] = df1['Unnamed: 17'].astype(str)

        df2 = pd.read_excel('ComparingSheets/commit.xlsx')
        df2['Original_Index'] = df2.index + 2
        df2['Country'] = df2['Country'].astype(str)
        df2['Unnamed: 17'] = df2['Unnamed: 17'].astype(str)

        merged_df = pd.merge(df1, df2, how='outer', indicator=True)
        differences = merged_df[merged_df['_merge'] != 'both']

        # Фильтрация только тех строк, которые есть во втором файле
        differences_in_second_file = differences[differences['_merge'] == 'right_only']
        # todo Раскоментить для получения заголовков

        indices = differences_in_second_file['Original_Index'].tolist()

        disciplines = differences_in_second_file['Discipline'].tolist()

        languages = differences_in_second_file['Language'].tolist()

        # orientations = differences_in_second_file['Orientation'].tolist()
        #
        # comments = differences_in_second_file['Comments'].tolist()

        countries = differences_in_second_file['Country'].tolist()

        # teams = differences_in_second_file['Team (Tricode)'].tolist()

        table_types = differences_in_second_file['Type'].tolist()

        links = differences_in_second_file['Link'].tolist()

        # durations = differences_in_second_file['Duration'].tolist()
        #
        # dates = differences_in_second_file['Date'].tolist()
        #
        # file_names = differences_in_second_file['File Name'].tolist()
        #
        # descriptions = differences_in_second_file['Description'].tolist()

        ready_status = differences_in_second_file['Status'] == "done"

        # hyperlinks_differences = extract_hyperlinks('ComparingSheets/commit.xlsx', column_index, indices)
        # links_dif = list(hyperlinks_differences.values())

        updates = []
        # Можно добавить в табличку булевый столбик статус и высылать результаты если столбик статус чем-то заполнен
        for index, discipline, language, table_type, link, country, ready in zip(indices, disciplines, languages,
                                                                                 table_types, links, countries,
                                                                                 ready_status):
            if (not pd.isna(index) and not pd.isna(discipline) and not pd.isna(language)
                    and not pd.isna(table_type) and not pd.isna(link) and ready):
                if pd.isna(country) or country == "nan":
                    country = "N/A"

                if "(" and ")" in discipline:
                    discipline = re.findall(r'\((.*?)\)', discipline)[0]

                if index in df1['Original_Index'].tolist():
                    text = (f"Update:\n Line: {index}\n Discipline: {discipline}\n Country: {country}\n "
                            f"Language: {language}\n Type: {table_type}\n Link: {link}")
                    print(text)
                    updates.append(text + "\n\n")
                else:
                    text = (f"Update:\n Line: {index}\n Discipline: {discipline}\n Country: {country}\n "
                            f"Type: {table_type}\n Link: {link}")
                    print(text)
                    updates.append(text + "\n\n")
            else:
                print(f"Line {index} has changed but it's no ready yet")

        os.remove('ComparingSheets/initial.xlsx')
        os.rename('ComparingSheets/commit.xlsx', 'ComparingSheets/initial.xlsx')
        return updates
    except IOError:
        await download_sheet("initial")
    except KeyError:
        os.remove('ComparingSheets/initial.xlsx')
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
            await asyncio.sleep(3)
        except KeyError:
            os.remove('ComparingSheets/initial.xlsx')
            await download_sheet("initial")
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