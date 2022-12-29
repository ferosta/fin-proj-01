# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% tags=[]
from rss_parser import Parser
from requests import get
import pandas as pd
import os 
import json
import time
from datetime import datetime, timezone

import sqlalchemy
from sqlalchemy import create_engine, MetaData,Table, Column, Numeric, Integer, VARCHAR, text
from sqlalchemy.engine import result


# %% [markdown] tags=[]
# # Конфигурационные настройки

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# ## Логирование

# %% tags=[]
# логирование
# PRJ_DIR = "" #'/home/fedorov/mypy/vk_prj/'
# if PRJ_DIR not in sys.path:
#     sys.path.insert(0, PRJ_DIR)
##########################################
# логирование
# лучше бы использовать loguru
import logging
import logging.config
dictLogConfig = {
    "version":1,
    "handlers":{
        "StreamHandler":{
            "class":"logging.StreamHandler",
            "formatter":"myFormatter"
        },
        "GlobalfileHandler":{
            "class":"logging.handlers.RotatingFileHandler",
            "formatter":"myFormatter",
            "filename": "LOG_.LOG",
            "backupCount": 10
        },
        "fileHandlerDEBUG":{
            "class":"logging.FileHandler",
            "formatter":"myFormatter",
            "filename": "DEBUG.LOG"
        },
         "fileHandlerINFO":{
            "class":"logging.FileHandler",
            "formatter":"myFormatter",
            "filename": "LOG_.LOG"
        },
    },
    "loggers":{
        "DEBUG":{
            "handlers":["fileHandlerDEBUG", "StreamHandler"],
            "level":"DEBUG",
        },
        "INFO":{
            "handlers":["fileHandlerINFO"],
            "level":"INFO",
        },
        "WARNING":{
            "handlers":["fileHandlerINFO", "GlobalfileHandler"],
            "level":"WARNING",
        },
        "ERROR":{
            "handlers":["fileHandlerINFO", "GlobalfileHandler"],
            "level":"ERROR",
        },
        "CRITICAL":{
            "handlers":["fileHandlerINFO", "GlobalfileHandler"],
            "level":"CRITICAL",
        }
    },
    "formatters":{
        "myFormatter":{
            "format":"%(asctime)s:%(name)s:%(levelname)s=>%(message)s<=%(filename)s->%(funcName)s[%(lineno)d]"
        }
    }
}
logging.config.dictConfig(dictLogConfig)


PROG_NAME = 'GET_RSS_DATA'
logger = logging.getLogger("INFO."+PROG_NAME)
# logger = logging.getLogger("DEBUG."+PROG_NAME)

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# ## Глобальные переменные

# %% tags=[]
# конфигурационные настройки
CONFIG_FILE_NAME = os.path.abspath(u'../config/rss_links.csv')
DATA_DIR_NAME = os.path.abspath(u'../data')

PGS_LGIN = 'postgres'
PGS_PSWD = 'postgres'
PGS_DB = 'postgres'
PGS_ADDR = ' 192.168.144.9' #172.17.0.1
PGS_PORT = 5440

SQL_ENGINE = create_engine(f'postgresql://{PGS_LGIN}:{PGS_PSWD}@localhost:{PGS_PORT}/{PGS_DB}')


# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# # Чтение конфига с адресами источников РСС

# %% tags=[]
# читаем конфиг со ссылками на источники
def read_config(CONFIG_FILE_NAME):
    """читаем конфиг со ссылками на источники
        CONFIG_FILE_NAME - имя файла с конфигом (если не в локальной директории то с путём)
    """
    df_config = pd.read_csv(CONFIG_FILE_NAME, header=None  )
    rss_urls = list(df_config[0])
    logger.debug(f'Ссылки на источники прочитаны из {CONFIG_FILE_NAME}')
    return rss_urls


# Тест
# rss_urls = read_config(CONFIG_FILE_NAME)
# rss_urls

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# # Подготовка первичного хранилища для данных из источников

# %% tags=[]
def rssname_to_dirname(rss_url:str):
    """ из адреса ссылки на источник делает имя папки для хранения фидов из этого источника
        Результат: название папки с фидами источника
    """
    # rss_url = 'https://regnum.ru/rss'# 'https://ria.ru/export/rss2/archive/index.xml' #'https://lenta.ru/rss/' # rss_urls[0]
    rss_dirname = rss_url.replace(u'https://', "").replace(u"/","|") 
    # abs_rss_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)
    return rss_dirname

    
# подготовить: проверить и если надо создать каталог под данные из источника
def rss_dir_prepare(rss_url):
    """ Проверить есть ли каталог для данного источника,
        Если нет, то создать каталог для сохранения сведений из источника .
        rss_url - ссылка на источник из конфиг-файла
    """
    # получаем имя папки с данными из ссылки на источник
    rss_dir_name = rssname_to_dirname(rss_url)# rss_url.replace(u'https://', "").replace(u"/","|")
    logger.debug(f'Проверяется папка rss_dir_name = {rss_dir_name}')
    
    # полный путь до папки с данными
    rss_full_dir_name = os.path.join(DATA_DIR_NAME , rss_dir_name ) 
    rss_abs_dir_name =  rss_full_dir_name #os.path.abspath(rss_full_dir_name)
    
    # если такой папки еще нет - то создаем
    if not os.path.exists(rss_abs_dir_name):
        os.mkdir(rss_abs_dir_name)
        logger.debug(f'Создна папка {rss_abs_dir_name}')
    
    return rss_abs_dir_name

# Тест:    
# rss_url = 'https://lenta.ru/rss/' # rss_urls[0]
# rss_dirname = rss_dir_prepare(rss_url)


# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# # Получение данных из источника по ссылке 

# %% tags=[]
# получение данных из источника по ссылке rss_url 
def get_rss(url : str):
    """ получение данных из источника по ссылке rss_url 
        Результат: словарь feed
    """
    # получаем данны из источника - всю порцию,которую он отдает. Настроек по выбору времени там нет!
    xml = get(url)
    parser = Parser(xml=xml.content  ) 
    feed = parser.parse()
    logger.debug(f'Данные из {url} получены. Кол-во записей: { len( feed.dict()["feed"]) }. Код Ок: {xml.ok}')
    return feed.dict()['feed']

# Тест:
# rss_url = 'https://lenta.ru/rss/' # rss_urls[0]
# rss_feed = get_rss(rss_url)


# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# # Сохранение полученных из истончика данных RSS в файл

# %% tags=[]

# преобразование даты из строки в datetime с timezone
def convert_to_tz_datetime(dt : str): 
    """ преобразование даты из строки в datetime с timezone
    """
    # формат даты #'Sat, 24 Dec 2022 09:10:22 +0300'   
    fmt = "%a, %d %b %Y %H:%M:%S %z" 
    # код таймзоны
    tz = datetime.strptime('+0300', '%z').tzinfo
    
    rez = datetime.now().astimezone(tz).strptime(dt, fmt)
    logger.debug(rez.strftime(fmt) )
    return rez


# сохранение полученного и распаршенного rss в файл
def save_rss_feed(feed_dict : dict, dir_to_save :str):
    """ сохранение полученного и распаршенного rss в файл
        вх: rss_feed - словарь с новостями
            dir_to_save - путь до директории сохранения
    """
    # формирование имени файла, в который записывается порция данных rss
    # текущий таймстамп - для уникального имени файла
    now_timestamp = int(datetime.now().timestamp())
    
    # #даты первой и последней новости в порции рсс
    # pub_date_to = convert_to_tz_datetime( rss_feed[0]['publish_date'] )
    # pub_date_from = convert_to_tz_datetime( rss_feed[-1]['publish_date'] )

    # #имя файла для сохранения порции рсс
    # fmt = "%Y-%m-%d_%H-%M-%S"
    # file_name_dic = {'to':pub_date_to.strftime(fmt), 'from': pub_date_from.strftime(fmt) }
    # file_name_str = json.dumps(file_name_dic).replace(": ",'|')
    # file_name_str
    
    # сохранение полученной порции rss в директорию источника

    # полное имя файла для записи
    abs_filename = os.path.join(dir_to_save, str(now_timestamp) + '.json')
    with open(abs_filename, mode="w") as fp:
        json.dump(feed_dict , fp )
        logger.debug(f'Rss_feed записан в файл {abs_filename}')
    
    return abs_filename

# Тест:    
# rss_filename = save_rss_feed(rss_feed, rss_dirname)


# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# # Загрузка данных из всех источников RSS и запись их в файлы

# %% tags=[]
def get_all_rss_data():
    """ Получение данных из всех источников и запись их в файлы"""
    logger.info('=== Начало загрузки данных ===')
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config(CONFIG_FILE_NAME)

    for url in rss_urls:

        # подготавливаем папки для хранения скачиваемых из РСС данных
        dirname = rss_dir_prepare(url)

        # получаем порцию данных по ссылке
        feed = get_rss(url)

        # сохраняем данные в заранее подготовленной папке
        rez_filename = save_rss_feed(feed, dirname)
        
    logger.info(f'=== Данные загрузили. Кол-во источников {len(rss_urls)} ===')
        

# Тест:
if "DEBUG" in logger.name:
    get_all_rss_data()


# %% [markdown] tags=[]
# # Инициализирующая Загрузка данных из файлов в хранилище (SQL БД)

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# ## Прочитать файл feed и сделать из него таблицу пандас

# %% tags=[]
# прочитать из фид-файла и записать в пандас датафрейм
def feedfile_to_pandas(rss_url:str, rss_file_name:str):
    """ Читает json файл с сохраненным feed и преобразует его в таблицу пандас
        rss_url - название папки с файлами-фидами источника
        rss_file_name - имя файла с фидом
        Результат: таблица пандас
    """
    
    # формируем полное имя файла
    rss_dirname = rssname_to_dirname(rss_url) 
    rss_full_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)
    feed_filename = os.path.join(rss_full_dirname, rss_file_name)
    
    
    # открываем первый файл - это самый новый, т.к. сотритовка обратная
    feed=''
    with open(feed_filename, 'r') as fp:
        feed = json.load(fp)
        logger.debug(f'Прочитали содержимое файла {feed_filename}. Кол-во записей: {len(feed)}')

    # закидываем фид в пандас : колонки только те, которые нужны
    columns = ['title', 'link', 'publish_date', 'category', 'description' ] # 'description_links', 'description_images', 'enclosure', 'itunes'
    df = pd.json_normalize(feed)[columns]
    # добавляем признак источника
    df['source'] = rss_dirname
    
    logger.debug(f'Из файла {feed_filename} получили таблицу, кол-во строк {len(df)}.')
    return df

# тест feedfile_to_pandas
# rss_url = 'https://regnum.ru/rss'
# feed_filename = '1672120674.json'
# df1 = feedfile_to_pandas(rss_url, feed_filename)
# df1

# %% [markdown] tags=[]
# ## ???(SQL) Начальная инициализация: Объединить все файлы из папки источника рсс в таблицы в БД

# %% tags=[]
# df0.to_sql('regnum.ru|rss'+'0', SQL_ENGINE, if_exists='replace')
# df1.to_sql('regnum.ru|rss'+'1', SQL_ENGINE, if_exists='replace')
# SQL_ENGINE.table_names()
# sql =  text("""
# SELECT * from "regnum.ru|rss" LIMIT 5
           
#            """)
# # results = SQL_ENGINE.execute(sql)
# # # View the records
# # for record in results:
# #     print("\n", record)



# %%
# def join_all_feedfiles_to_SQL(rss_url: str):
#     """" Взять все файлы с фидами в папке рсс и объединить их, убрав повторения, записав в основное хранилище SQL
#         Результат: готовая начальная SQL-таблица
#     """
#     # подготавливаем имя папки для чтения скачанных из РСС данных - отдельных файлов
#     rss_dirname = rssname_to_dirname(rss_url) #rss_url.replace(u'https://', "").replace(u"/","|") # rss_dir_prepare(rss_url)
#     abs_rss_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)
    
#     # получаем список сохраненных файлов
#     list_dir = [ fn for fn in sorted( os.listdir(abs_rss_dirname), reverse=True) if '.json' in fn]
#     logger.debug(f'Прочитали директорию {abs_rss_dirname}. Кол-во файлов: {len(list_dir)}. Список: {list_dir}')
    
    
#     #подготовка таблицы в БД
#     engine = 
    
#     for rf in list_dir:
#         # получаем датафрейм пандас для файла
#         df = feedfile_to_pandas(rss_url, rf)
        
#         #для отладки инфо: превая и последняя запись датафрефма
#         str_fst = df.iloc[0,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
#         str_lst = df.iloc[-1,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
#         logger.debug(f'Таблица для файла:{rf}, строк:{len(df)}, нач.:{str_fst}, кон.:{str_lst}')
#         # объединяем полученное с имеющимся 
#         # if df_rez.empty:
#         #     df_rez = df
#         #     logger.debug(f'Начальная инициализация пустой таблицы')
#         df_rez = pd.concat([df_rez, df], ignore_index=True )
        
#         df.to_sql(,SQL_ENGINE)
        
        
    
    
#     logger.debug(f'Сформировали сводную таблицу для файлов в {abs_rss_dirname}. Кол-во строк: {len(df_rez)}')
#     df_rez.drop_duplicates(ignore_index=True, inplace=True)
#     logger.debug(f'После удаления дубликатов: кол-во строк: {len(df_rez)}')
    
#     return df_rez

# %% [markdown] tags=[]
# ## 1. (Pandas) Начальная инициализация: Объединить все файлы из папки источника рсс и записать результат в хранилище

# %% tags=[]
def join_all_feedfiles_pandas_sql(rss_url: str):
    """ взять все файлы с фидами в папке рсс, объединить их, убрав повторения и приготовить к записи в хранилище (?БД)
        Результат: таблица пандас с уникальными записями из всех файлов в папке источника
    """
    # подготавливаем имя папки для чтения скачанных из РСС данных - отдельных файлов
    rss_dirname = rssname_to_dirname(rss_url) #rss_url.replace(u'https://', "").replace(u"/","|") # rss_dir_prepare(rss_url)
    abs_rss_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)
    
    # получаем список сохраненных файлов
    list_dir = [ fn for fn in sorted( os.listdir(abs_rss_dirname), reverse=True) if '.json' in fn]
    logger.debug(f'Прочитали директорию {abs_rss_dirname}. Кол-во файлов: {len(list_dir)}. Список: {list_dir}')
    
    df_rez = pd.DataFrame()
    
    for rf in list_dir:
        # получаем датафрейм пандас для файла
        df = feedfile_to_pandas(rss_url, rf)
        #дату из строки делаем датой
        df['publish_date'] = pd.to_datetime(df['publish_date'])
        
        #для отладки инфо: превая и последняя запись датафрефма
        str_fst = df.iloc[0,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
        str_lst = df.iloc[-1,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
        logger.debug(f'Таблица для файла:{rf}, строк:{len(df)}, нач.:{str_fst}, кон.:{str_lst}')
        # объединяем полученное с имеющимся 
        if df_rez.empty:
            df_rez = df
            logger.debug(f'Начальная инициализация пустой таблицы')
        df_rez = pd.concat([df_rez, df], ignore_index=True )
    
    logger.debug(f'Сформировали сводную таблицу для файлов в {abs_rss_dirname}. Кол-во строк: {len(df_rez)}')
    df_rez.drop_duplicates(ignore_index=True, inplace=True)
    logger.debug(f'После удаления дубликатов: кол-во строк: {len(df_rez)}')
    
    # добавляем результат в БД
    df_rez.to_sql(rss_dirname, SQL_ENGINE, if_exists='replace')
    logger.debug(f'Добавлено в БД в таблицу: {rss_dirname}')
    
    return df_rez

# # тест
# rss_url = 'https://regnum.ru/rss'#
# df_rez = join_all_feedfiles_pandas_sql(rss_url)
    
    

# %% [markdown] tags=[]
# ## Загрузка данных из всех файлов всех папок источников RSS в SQL через pandas

# %% jupyter={"outputs_hidden": true} tags=[]
def load_all_feeddirs_to_sql():
    """ Загрузка всех данных из папок источников в SQL , через объединение их в pandas"""
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config(CONFIG_FILE_NAME)

    for url in rss_urls:

        # группируем все в один датафрейм и записываем его в SQL
        join_all_feedfiles_pandas_sql(url)


if "DEBUG" in logger.name:
    # можно сначала загрузить свежую порцию фидов 
    # get_all_rss_data()
    # а потом закинуть все в БД
    load_all_feeddirs_to_sql()


# %% [markdown]
# ## 2. ? Тест: Взять самый свежий файл и следующий за ним файл и объединить, убрав повторения

# %%
# def join_two_feeds(rss_url:str, rss_file_name1:str, rss_file_name2:str):
#     """ Сливает два фида, представленных таблицами пандас, в один , удаляя повторения
#     """
    
#     df1 = feedfile_to_pandas(rss_url, rss_file_name1)
#     df2 = feedfile_to_pandas(rss_url, rss_file_name2)
#     df_rez = pd.concat([df1, df2] )#,ignore_index=True
#     df_rez.drop_duplicates(inplace=True)
    
#     return df_rez


# %% tags=[]
# # подготавливаем папки для хранения скачиваемых из РСС данных
# rss_url = 'https://regnum.ru/rss'# 'https://ria.ru/export/rss2/archive/index.xml' #'https://lenta.ru/rss/' # rss_urls[0]
# rss_dirname = rssname_to_dirname(rss_url) #rss_url.replace(u'https://', "").replace(u"/","|") # rss_dir_prepare(rss_url)
# abs_rss_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)

# # получаем список сохраненных файлов
# list_dir = [ fn for fn in sorted( os.listdir(abs_rss_dirname), reverse=True) if '.json' in fn]
# logger.debug(f'Прочитали директорию {abs_rss_dirname}. Файлов: {len(list_dir)}. Список: {list_dir}')

# #самый свежий файл
# file0 = list_dir[0]
# df0 = feedfile_to_pandas(rss_url, file0)
# # открываем следующий файл - это файл чуть старее,чем первый
# file1 = list_dir[1]
# df1 = feedfile_to_pandas(rss_url, file1)

# df0.iloc[0:5,:]

# df1.iloc[0:5,:]

# dfx = df0.merge(df1, on=['title', 'link', 'publish_date', 'category', 'description', 'source' ], how='right', indicator= True ).dropna()
# dfx[dfx._merge == 'right_only' ]

# df_rez = pd.concat([df0, df1] )#,ignore_index=True
# df_rez

# df_rez.drop_duplicates()

# %% [markdown]
# # Инкрементальная загрузка данных из RSS

# %%
""" Вариант1:
    Скачать порцию данных
    Преобразовать ее в пандас
    Получить самую свежую запись из БД
    Определить в таблице пандас записи более новые, чем самая свежая из БД
    Дописать полученные записи в БД
"""

# %%

# %% [markdown]
# # Группировка тематических рубрик

# %% [markdown]
# ## Тематическое моделирование

# %%
