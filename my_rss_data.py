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

# %% [markdown] tags=[]
# # Библиотеки

# %% tags=[]
from rss_parser import Parser
from requests import get
import pandas as pd
import os 
import json
import time
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData,Table, Column, Numeric, Integer, VARCHAR, text, DateTime 
from sqlalchemy.engine import result

import errno


# %% [markdown] tags=[]
# # Конфигурационные настройки

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[] jp-MarkdownHeadingCollapsed=true
# ## Глобальные переменные

# %% tags=[]
#импортируем путь и уровень лога - они разные для отладки и продакшына
from my_rss_data_env import RUN_DIR, LOG_LEVEL

# название программы - для логов
PROG_NAME = 'MY_RSS_DATA'
# LOG_LEVEL = 'INFO' # 'DEBUG' # 
RUN_DIR = os.path.abspath(RUN_DIR)

# конфигурационные настройки
CONFIG_FILE_NAME = os.path.join(RUN_DIR, u'config/rss_links.csv') 
DATA_DIR_NAME = os.path.join(RUN_DIR, u'data')
MAIN_TABLE_NAME = "main"
CATEGORY_FILE = os.path.join(RUN_DIR, u'./category/category.csv')
CATEGORY_TABLE = "category_map"


# подключение к Postgres - который развернут в докере на сервере
PGS_LGIN = 'postgres'
PGS_PSWD = 'postgres'
PGS_DB = 'postgres'
PGS_ADDR =  '172.17.0.1' #'192.168.144.9'
PGS_PORT = 5440

SQL_ENGINE = create_engine(f'postgresql://{PGS_LGIN}:{PGS_PSWD}@localhost:{PGS_PORT}/{PGS_DB}')

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# ## Логирование

# %% tags=[]
##########################################
# логирование
# !!! лучше бы использовать loguru !!!
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
            "filename": f"LOG_{PROG_NAME}.LOG",
            "backupCount": 10
        },
        "fileHandlerDEBUG":{
            "class":"logging.FileHandler",
            "formatter":"myFormatter",
            "filename": f"DEBUG_{PROG_NAME}.LOG"
        },
         "fileHandlerINFO":{
            "class":"logging.FileHandler",
            "formatter":"myFormatter",
            "filename": f"LOG_{PROG_NAME}.LOG"
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


logger = logging.getLogger(f'{LOG_LEVEL}.{PROG_NAME}')
# logger = logging.getLogger("DEBUG."+PROG_NAME)

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# # Чтение конфига с адресами источников РСС

# %% tags=[]
# читаем конфиг со ссылками на источники
def read_config(config=CONFIG_FILE_NAME):
    """читаем конфиг со ссылками на источники
        config - имя файла с конфигом (если не в локальной директории то с путём)
    """
    df_config = pd.read_csv(config, header=None  )
    rss_urls = list(df_config[0])
    logger.debug(f'Ссылки на источники прочитаны из {config}')
    return rss_urls


# Тест
rss_urls = read_config()
rss_urls


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
# # ** Загрузка данных из всех источников RSS и запись их в файлы

# %% tags=[]
def get_all_rss_data():
    """ Получение данных из всех источников и запись их в файлы
        Для CRONa
    """
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
        

# # Тест:
# if "DEBUG" in logger.name:
#     get_all_rss_data()

# %% [markdown] tags=[]
# # Инициализирующая Загрузка данных из файлов в хранилище (SQL БД)

# %% [markdown] jp-MarkdownHeadingCollapsed=true tags=[]
# ## Прочитать файл feed и сделать из него таблицу пандас

# %% tags=[]
# прочитать из фид-файла и записать в пандас датафрейм
def feedfile_to_pandas(rss_url:str, rss_file_name:str):
    """ Читает json файл с сохраненнымto_list преобразует его в таблицу пандас
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
    df['publish_date'] = pd.to_datetime(df['publish_date'])
    df['hash'] = pd.util.hash_pandas_object(df[['title','category', 'source']]).astype('str')
    
    logger.debug(f'Из файла {feed_filename} получили таблицу, кол-во строк {len(df)}.')
    return df

# тест feedfile_to_pandas
# rss_url = 'https://regnum.ru/rss'
# feed_filename = '1672120674.json'
# df1 = feedfile_to_pandas(rss_url, feed_filename)
# df1

# %% [markdown] tags=[] jp-MarkdownHeadingCollapsed=true
# ## 1. -- Начальная инициализация через Pandas: Объединить в Pandas все файлы из папки источника рсс и записать результат в хранилище

# %% [raw] tags=[]
# def join_all_feedfiles_pandas_sql(rss_url: str):
#     """ взять все файлы с фидами в папке рсс, объединить их, убрав повторения и приготовить к записи в хранилище (?БД)
#         Результат: таблица пандас с уникальными записями из всех файлов в папке источника
#     """
#     # подготавливаем имя папки для чтения скачанных из РСС данных - отдельных файлов
#     rss_dirname = rssname_to_dirname(rss_url) #rss_url.replace(u'https://', "").replace(u"/","|") # rss_dir_prepare(rss_url)
#     abs_rss_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)
#     
#     # получаем список сохраненных файлов
#     list_dir = [ fn for fn in sorted( os.listdir(abs_rss_dirname), reverse=True) if '.json' in fn]
#     logger.debug(f'Прочитали директорию {abs_rss_dirname}. Кол-во файлов: {len(list_dir)}. Список: {list_dir}')
#     
#     df_rez = pd.DataFrame()
#     
#     for rf in list_dir:
#         # получаем датафрейм пандас для файла
#         df = feedfile_to_pandas(rss_url, rf)
#         #дату из строки делаем датой
#         # df['publish_date'] = pd.to_datetime(df['publish_date'])
#         
#         #для отладки инфо: превая и последняя запись датафрефма
#         str_fst = df.iloc[0,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
#         str_lst = df.iloc[-1,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
#         logger.debug(f'Таблица для файла:{rf}, строк:{len(df)}, нач.:{str_fst}, кон.:{str_lst}')
#         # объединяем полученное с имеющимся 
#         if df_rez.empty:
#             df_rez = df
#             logger.debug(f'Начальная инициализация пустой таблицы')
#         df_rez = pd.concat([df_rez, df], ignore_index=True ).df_rez.drop_duplicates(ignore_index=True)
#     
#     logger.debug(f'Сформировали сводную таблицу для файлов в {abs_rss_dirname}. Кол-во строк: {len(df_rez)}')
#     # df_rez.drop_duplicates(ignore_index=True, inplace=True)
#     # logger.debug(f'После удаления дубликатов: кол-во строк: {len(df_rez)}')
#     
#     # добавляем результат в БД
#     df_rez.to_sql(rss_dirname, SQL_ENGINE, if_exists='replace' )
#     logger.debug(f'Добавлено в БД в таблицу: {rss_dirname}')
#     
#     return df_rez
#
# # # тест
# # if "DEBUG" in logger.name:
#     # rss_url = 'https://regnum.ru/rss'#
#     # df_rez = join_all_feedfiles_pandas_sql(rss_url)
#     
#     

# %% [markdown] tags=[]
# ## 2. ** Начальная инициализация через SQL: Каждый файлы из папки источника рсс добавить в SQL хранилище, убрав дубликаты

# %% tags=[]
def insert_all_feedfiles_sql(rss_url: str):
    """ брать по очереди файлы с фидами в папке рсс и вставлять в SQL таблицу, избегая повторений
        Результат: SQL таблица с уникальными записями из всех файлов в папке источника
        
        от insert_newest_feedfiles_by_sql отличается только тем, что берутся все файлы , а там только превый.
        Можно сделать одну функцию и через параметр управлять количеством файлов. 
    """
    # подготавливаем имя папки для чтения скачанных из РСС данных - отдельных файлов
    rss_dirname = rssname_to_dirname(rss_url) #rss_url.replace(u'https://', "").replace(u"/","|") # rss_dir_prepare(rss_url)
    abs_rss_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)
    
    # поскольку это начальная инициализация, то имеющуюся SQL таблицу удаляем
    q = f'DROP TABLE IF EXISTS "{rss_dirname}"'
    with SQL_ENGINE.connect() as con:
            res = con.execute(q)
    
    # получаем список сохраненных файлов
    list_dir = [ fn for fn in sorted( os.listdir(abs_rss_dirname), reverse=True) if '.json' in fn]
    logger.debug(f'Прочитали директорию {abs_rss_dirname}. Кол-во файлов: {len(list_dir)}. Список: {list_dir}')
    
    for rf in list_dir:
        # получаем датафрейм пандас для файла
        df = feedfile_to_pandas(rss_url, rf)
                
        # #для отладки инфо: превая и последняя запись датафрефма
        # str_fst = df.iloc[0,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
        # str_lst = df.iloc[-1,:][['publish_date', 'title']].to_string().replace('  ',"").replace('publish_date',"").replace('\ntitle',"")[:50]
        # logger.debug(f'Таблица для файла:{rf}, строк:{len(df)}, нач.:{str_fst}, кон.:{str_lst}')
        # объединяем полученное с имеющимся 
        # if df_rez.empty:
        #     df_rez = df
        #     logger.debug(f'Начальная инициализация пустой таблицы')
        # df_rez = pd.concat([df_rez, df], ignore_index=True ).df_rez.drop_duplicates(ignore_index=True)
        
        # если sql таблица с данными для этого источника еще не создана - создаем новую, иначе - дописываем
        if rss_dirname not in sa.inspect(SQL_ENGINE).get_table_names():
            # вставляем данные из пандаса прямо в новую создаваемуб ОСНОВНУЮ таблицу для данного источника
            df.to_sql(rss_dirname, SQL_ENGINE, if_exists='replace', index=False)
            # делаем первичным ключем - хэш, чтобы записи не повторялись
            q = f'ALTER TABLE public."{rss_dirname}" ADD CONSTRAINT "{rss_dirname}_pk" PRIMARY KEY (hash);'
            
        else:
            # вставляем данные из пандаса прямо в новую создаваемуб ВРЕМЕННУЮ таблицу
            tmp_dbname = "tmp."+rss_dirname
            df.to_sql(tmp_dbname, SQL_ENGINE, if_exists='replace', index=False)
            q = f'INSERT INTO "{rss_dirname}" SELECT * FROM "{tmp_dbname}"\
                                                WHERE hash NOT IN (SELECT hash FROM "{rss_dirname}");\
                                                DROP TABLE "{tmp_dbname}"'
            
        # выполняем сформированный SQl запрос
        with SQL_ENGINE.connect() as con:
            res = con.execute(q)
            

    res = SQL_ENGINE.execute(f'SELECT count(*) FROM "{rss_dirname}"')
    
    # общее количество строк в таблице
    num_str = res.first()[0]
            
    logger.debug(f'Сформировали SQL таблицу "{rss_dirname}". Кол-во строк: {num_str}')
    
    return {rss_dirname:num_str}

# # тест
# rez_sql = ''
# if "DEBUG" in logger.name:
#     rss_url = 'https://regnum.ru/rss'#
#     rez_sql = insert_all_feedfiles_sql(rss_url)
# rez_sql


# %% [markdown] tags=[]
# ## 3. ** Инкрементальная загрузка свежей порции данных через SQL

# %% tags=[]
def insert_newest_feedfiles_by_sql(rss_url: str):
    """ взять самый свежий файл с фидом в папке рсс и встить в SQL таблицу, избегая повторений
        Результат: SQL таблица с уникальными записями из всех файлов в папке источника
    """
    # подготавливаем имя папки для чтения скачанных из РСС данных - отдельных файлов
    rss_dirname = rssname_to_dirname(rss_url) #rss_url.replace(u'https://', "").replace(u"/","|") # rss_dir_prepare(rss_url)
    abs_rss_dirname = os.path.join(DATA_DIR_NAME, rss_dirname)
    
    # надо бы проверить - существует такакая таблица или еще нет
    
    # получаем список сохраненных файлов - сортируем в порядке убывания времени - т.е. самый свежий файл будет первым
    list_dir = [ fn for fn in sorted( os.listdir(abs_rss_dirname), reverse=True) if '.json' in fn]
    # если файлов не нашлось - страшно ругаемся
    if len(list_dir) == 0:
        logger.error("Стоп! Файлы для добавления в sql таблицу {rss_dirname} отсутствуют в папке {abs_rss_dirname}")
        raise IOError 
    logger.debug(f'Прочитали директорию {abs_rss_dirname}. Берем в работу самый свежий файл: {list_dir[0]}')
    
    for rf in list_dir[0:1]:
        # получаем датафрейм пандас для файла
        df = feedfile_to_pandas(rss_url, rf)
        
        # если sql таблица с данными для этого источника еще не создана - создаем новую, иначе - дописываем
        if rss_dirname not in sa.inspect(SQL_ENGINE).get_table_names():
            # вставляем данные из пандаса прямо в новую создаваемуб ОСНОВНУЮ таблицу для данного источника
            df.to_sql(rss_dirname, SQL_ENGINE, if_exists='replace', index=False)
            # делаем первичным ключем - хэш, чтобы записи не повторялись
            q = f'ALTER TABLE public."{rss_dirname}" ADD CONSTRAINT "{rss_dirname}_pk" PRIMARY KEY (hash);'
            
        else:
            # вставляем данные из пандаса прямо в новую создаваемуб ВРЕМЕННУЮ таблицу
            tmp_dbname = "tmp."+rss_dirname
            df.to_sql(tmp_dbname, SQL_ENGINE, if_exists='replace', index=False)
            q = f'INSERT INTO "{rss_dirname}" SELECT * FROM "{tmp_dbname}"\
                                                WHERE hash NOT IN (SELECT hash FROM "{rss_dirname}");\
                                                DROP TABLE "{tmp_dbname}"'
            
        # выполняем сформированный SQl запрос
        with SQL_ENGINE.connect() as con:
            res = con.execute(q)
            

    res = SQL_ENGINE.execute(f'SELECT count(*) FROM "{rss_dirname}"')
    
    # финальное количство строк в таблице
    num_str = res.first()[0]
    
    logger.debug(f'Записали в SQL таблицу "{rss_dirname}". Кол-во строк: {num_str}')
    
    return {rss_dirname:num_str}

# # тест
# rez_sql = ''
# if "DEBUG" in logger.name:
#     rss_url = 'https://regnum.ru/rss'#
#     rez_sql = insert_newest_feedfiles_by_sql(rss_url)
# rez_sql


# %% [markdown] tags=[] jp-MarkdownHeadingCollapsed=true
# ## -- Инициализирующая Загрузка данных из всех файлов всех папок источников RSS в SQL через PANDAS

# %% tags=[]
def load_all_feeddirs_to_sql_by_pandas():
    """ Загрузка всех данных из папок источников в SQL , через объединение их в pandas"""
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config(CONFIG_FILE_NAME)

    for url in rss_urls:

        # группируем все в один датафрейм и записываем его в SQL
        join_all_feedfiles_pandas_sql(url)


# if "DEBUG" in logger.name:
#     # можно сначала загрузить свежую порцию фидов 
#     # get_all_rss_data()
#     # а потом закинуть все в БД
#     load_all_feeddirs_to_sql_by_pandas()


# %% [markdown] tags=[]
# ## ** Инициализирующая Загрузка данных из ВСЕХ файлов всех папок источников RSS СРАЗУ в SQL

# %% tags=[]
def load_all_feeddirs_directly_to_sql():
    """ Инициализирующая Загрузка всех данных из папок источников непосресдвенно в SQL 
        Если таблица уже была, то она удаляется
    """
    
    logger.info(f'== Начало Инициализирующей загрузки')
    
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config(CONFIG_FILE_NAME)

    str_num = dict()
    
    for url in rss_urls:
        # группируем все в один датафрейм и записываем его в SQL
        rez = insert_all_feedfiles_sql(url) 
        str_num.update(rez)
        
    logger.info(f'== Инициализирующая загрузка произведена. Кол-ва загруженных строк: {str_num}')
        


# if "DEBUG" in logger.name:
#     # можно сначала загрузить свежую порцию фидов 
#     # get_all_rss_data()
#     # а потом закинуть все в БД
#     load_all_feeddirs_directly_to_sql()


# %% [markdown] tags=[]
# ## ** Инкрементальная Загрузка данных для всех источников RSS СРАЗУ в SQL

# %% tags=[]
def load_newest_feeddirs_directly_to_sql():
    """ Загрузка самых новых данных (самый ноывй файл) из папок источников непосредственно в SQL  """
    
    logger.info('=== Запись свежих данных в SQL таблицы ===')
    
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config(CONFIG_FILE_NAME)

    str_num = dict()
    
    for url in rss_urls:
        # группируем все в один датафрейм и записываем его в SQL
        rez = insert_newest_feedfiles_by_sql(url)
        str_num.update(rez)
        
    logger.info(f'=== Конец записи. Кол-ва записей в таблицах: {str_num} ===')
    

# if "DEBUG" in logger.name:
#     # можно сначала загрузить свежую порцию фидов 
#     # get_all_rss_data()
#     # а потом закинуть все в БД
#     load_newest_feeddirs_directly_to_sql()


# %% [markdown] tags=[]
# # ** CRON : регулярное получение данных и записывание их в SQL базу

# %% tags=[]
def cron():
    """ реуглярно собираем данные из источников и тут же записываем их в SQL"""
    get_all_rss_data()
    
    load_newest_feeddirs_directly_to_sql()
    
# тест
if "DEBUG" in logger.name:
    cron()


# %% [markdown]
# # Получение объединенной SQL таблицы для всех источников

# %% tags=[]
def make_union_main_table(main_table=MAIN_TABLE_NAME):
    """ сливает все имеющиеся таблицы с данными из источников в одну - 
        имя главной таблицы по умолчанию MAIN_TABLE_NAME
    """
    
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config()
    
    # если пришел пустой список - страшно ругаемся 
    if len(rss_urls) == 0:
        logger.error(f'Стоп! Список источников пуст: {len(rss_urls)}')
        raise IOError
        
    rss_tablenames = [rssname_to_dirname(url) for url in rss_urls]
    
    
    # не удалось использовать SELECT * INTO ... в DBeaver работает, а здесь нет..
    # приходится вручную создавать таблицу
    # !!! если здесь добавить в конце COMMIT; то будет работать как в DBeaver!!!
    qc = f'CREATE TABLE "{main_table}" (\
    title text NULL,\
    link text NULL,\
    publish_date timestamptz NULL,\
    category text NULL,\
    description text NULL,\
    "source" text NULL,\
    hash text NOT NULL,\
    CONSTRAINT "main_table_pk" PRIMARY KEY (hash)\
    );'
    
    q = f'DROP TABLE IF EXISTS "{main_table}";'
          # DROP CONSTRAINT IF EXISTS "main_table_pk";'
    
    qq = f'INSERT INTO "{main_table}" SELECT * FROM "{rss_tablenames[0]}" '
     
    qqq = ' '.join( [f'UNION SELECT * FROM "{u}" ' for u in rss_tablenames[1:]] )
    
    qqqq = qq + qqq + ' ;'
    
    # with SQL_ENGINE.connect() as con:
    #     res = con.execute(q)
    #     res = con.execute(qqqq)
    res = SQL_ENGINE.execute(q)
    res = SQL_ENGINE.execute(qc)
    res = SQL_ENGINE.execute(qqqq)
    
    res = SQL_ENGINE.execute(f'SELECT count(*) FROM "{main_table}"')
    # общее количество строк в таблице
    num_str = res.first()[0]
        
        
    logger.debug(f'Создали объединенную таблицу {main_table}. Количетво записей: {num_str}')
    
    ##должно получаться как-то вот так
    #     SELECT * INTO main 
    # 	  		   FROM "habr.com|ru|rss|all|all|"
    # UNION SELECT * FROM "hibinform.ru|feed|"
    # UNION SELECT * FROM "lenta.ru|rss|"
    # UNION SELECT * FROM "regnum.ru|rss"
    # UNION SELECT * FROM "ria.ru|export|rss2|archive|index.xml"
    # UNION SELECT * FROM "rossaprimavera.ru|rss"
    # UNION SELECT * FROM "tass.ru|rss|v2.xml"
    # UNION SELECT * FROM "www.cnews.ru|inc|rss|news.xml"
    # UNION SELECT * FROM "www.kommersant.ru|RSS|news.xml"
    # UNION SELECT * FROM "www.vedomosti.ru|rss|news"
        
    # logger.debug(f'Строка запроса: {qqqq} ===')
    # return qqqq
        

# # Тест:
# q=''
# qq=''
# if "DEBUG" in logger.name:
#      make_union_main_table()


# %% [markdown]
# # Группировка тематических рубрик

# %% [markdown]
# ## загрузка групп категорий из файла в таблицу SQL

# %% tags=[]
def load_category_map_from_file(cat_file =CATEGORY_FILE, cat_tab=CATEGORY_TABLE):
    """ формирование таблицы сводных категорий из внешнего файла
        .. пока такой вариант.
    """
    df = pd.read_csv(cat_file, sep=';', header=None).rename(columns={0:'category', 1:'cat_group'})
    
    # если пришел пустой список - страшно ругаемся 
    if len(df) == 0:
        logger.error(f'Стоп! Список категорий в файле {cat_file} пуст: {len(rss_urls)}')
        raise IOError
    
    logger.debug(f'Прочитан файл сводных категорий. Кол-во записей: {len(df)}')
    
    df.to_sql(cat_tab, SQL_ENGINE, if_exists='replace', index=False)
    
#     # ключ
#     q = f'ALTER TABLE public."{cat_tab}" ADD CONSTRAINT "{cat_tab}_pk" PRIMARY KEY (category);'
#     res = SQL_ENGINE.execute(q)

    # добавление суррогатного ключа
    q = f'ALTER TABLE {cat_tab} ADD COLUMN id SERIAL PRIMARY KEY;'
    res = SQL_ENGINE.execute(q)
    
    # считаем общее количество получившихся строк
    res = SQL_ENGINE.execute(f'SELECT count(*) FROM "{cat_tab}"')
    # общее количество строк в таблице
    num_str = res.first()[0]
    
    
    logger.debug(f'Сводные категории загружены в SQL таблицу {cat_tab}. Кол-во записей: {num_str}')
    
    return  df

#тест
df=''
if "DEBUG" in logger.name:
    df = load_category_map_from_file()
df   


# %% [markdown]
# ## Добавление к главной обобщающей таблице групп категорий

# %% tags=[]
def add_cat_group_to_main_table():
    """
        Добавление к главной обобщающей таблице групп категорий
    """
     
    
    res = SQL_ENGINE.execute(f'\
            DROP TABLE IF EXISTS "{MAIN_TABLE_NAME}_cat";')
            # DROP CONSTRAINT IF EXISTS "main_table_cat_pk";')
    logger.debug(f'Удаление главной таблицы: {res}')

    # формируем новую главную таблицу для вставки туда категрий
    res = SQL_ENGINE.execute(f'\
        CREATE TABLE "{MAIN_TABLE_NAME}_cat" (\
            title text  NULL,\
            link text  NULL,\
            publish_date timestamptz  NULL,\
            category text NULL,\
            description text  NULL,\
            "source" text  NULL,\
            hash text  NULL,\
            cat_group text NULL,\
            cat_id INTEGER NULL,\
            CONSTRAINT "main_table_cat_pk" PRIMARY KEY (hash)\
        );\
    ')
    logger.debug(f'Создание новой главной таблицы. Кол-во записей: {res.rowcount}')

    # вставляем в основную таблицу категрии новостей
    res = SQL_ENGINE.execute(f'\
        INSERT INTO "{MAIN_TABLE_NAME}_cat" \
        SELECT m.*, cm.cat_group, cm.id as cat_id FROM "{MAIN_TABLE_NAME}" m \
        LEFT JOIN "{CATEGORY_TABLE}" cm  ON m.category = cm.category;\
    ')
    logger.debug(f'Добавеление в главную таблицу категрий. Кол-во записей: {res.rowcount}')
    
    
    # т.к. в текущей версии костыльных ручных групп новостей будут не все группы, то 
    # пустые группы новостей пронумеруем и поименуем вручную еще раз
    res = SQL_ENGINE.execute(f"\
        UPDATE {MAIN_TABLE_NAME}_cat \
        SET cat_group = 'новые новости' \
            ,cat_id = 0 \
        WHERE cat_group IS NULL OR cat_id IS NULL; \
    ")
    logger.debug(f'Корректировка неучтенных категорий. Кол-во записей: {res.rowcount}')

    # q = f'INSERT INTO "{MAIN_TABLE_NAME}_cat" \
    # (SELECT m.*, cm.cat_group FROM "{MAIN_TABLE_NAME}" m \
    # LEFT JOIN "{CATEGORY_TABLE}" cm  ON m.category = cm.category) \
    # UNION ALL \
    # (SELECT m.*, cm.category as "cat_group" FROM "{MAIN_TABLE_NAME}" m, "{CATEGORY_TABLE}" cm  \
    # WHERE  m.category != cm.category)\
    # ;'

    # print(q)
    logger.debug(f'Сформирована главная таблица с категориями. Всего записей: {res.rowcount}')
    return res
    
# #тест
# res = ''
# if "DEBUG" in logger.name:
#     res = add_cat_group_to_main_table()
# # print (res)


# %% [markdown]
# ## --Тематическое моделирование

# %%

# %% [markdown]
# # Витрина

# %% [markdown]
# ## вар1: ПредВитрина №№01-02
# Суррогатный ключ категории
# Название категории
# Общее количество новостей из всех источников по данной категории за все время
# Количество новостей данной категории для каждого из источников за все время

# %% tags=[]
def make_vitrine_01_02(vitrine_name='vitrine_03'):
    """
        -- Промежуточный результат: Витрина с данными задачи №01 и №02
        -- выполнение одним запросом без сохранения промежуточных таблиц
    """
    # TODO: надо бы еще формирование списков источников сделать динамическим
    #
    q = f"""
        DROP TABLE IF EXISTS {vitrine_name};
        WITH 
         cat_list AS (SELECT cat_group AS "Категория" FROM {MAIN_TABLE_NAME}_cat GROUP BY "Категория" ORDER BY "Категория")-- список категорий
        ,cat_nlist AS(SELECT ROW_NUMBER() OVER() AS "N", "Категория"   FROM cat_list) -- нумерованный список категорий
        ,all_src AS (SELECT * FROM -- все категории по всем источникам за все время
                        crosstab('SELECT cat_group AS "Категория", ''Все источники'' AS "Источник", count(*) AS "Всего новостей"
                                  FROM {MAIN_TABLE_NAME}_cat GROUP BY cat_group ORDER BY cat_group'
                        ) AS ct ("Категория_01" text, "Все источники" INT8))
        ,each_src AS (SELECT * FROM -- все категории по каждому источнику за все время
                        crosstab('SELECT cat_group AS "Категория", "source" as "Источник", count(*) AS "Всего новостей"
                                  FROM {MAIN_TABLE_NAME}_cat GROUP BY "Категория", "Источник" ORDER BY 1, 2'
                        , 'SELECT DISTINCT "source" FROM {MAIN_TABLE_NAME}_cat ORDER BY "source"'
                        ) AS ct ("Категория_02" TEXT
                                ,"habr.com|ru|rss|all|all|" int8
                                ,"hibinform.ru|feed|" int8
                                ,"lenta.ru|rss|" INT8
                                ,"regnum.ru|rss" INT8
                                ,"ria.ru|export|rss2|archive|index.xml" INT8
                                ,"rossaprimavera.ru|rss" INT8
                                ,"tass.ru|rss|v2.xml" INT8
                                ,"www.cnews.ru|inc|rss|news.xml" INT8
                                ,"www.kommersant.ru|RSS|news.xml" INT8
                                ,"www.vedomosti.ru|rss|news" INT8
                                ))
        SELECT cn."N", als.*, eas.* 
        INTO {vitrine_name}  -- сохранение в таблицу
        FROM cat_nlist cn -- нумерованный список категорий
        LEFT JOIN 
            (SELECT * FROM all_src) AS als
        ON cn."Категория" = als."Категория_01"
        LEFT JOIN 
            (SELECT * FROM each_src) AS eas
        ON als."Категория_01" = eas."Категория_02";
        COMMIT;
     """
    
    
    res = SQL_ENGINE.execute(q)
    # conn = SQL_ENGINE.connect()
    # res = conn.execute(q)
    # res = conn.execute("""SELECT * FROM vitrine_03""")
    # conn.commit_prepared()
    # conn.close()
    
    # logger.debug(f'суппер-пуупер запрос: {res.rowcount}')

   
    logger.debug(f'Сформирована витрина ({vitrine_name}) по п.1 и п.2. Всего записей: {res.rowcount}')
    # logger.debug(f'записей: {res.all()}')
    
    return res
    
# #тест
# res = ''
# if "DEBUG" in logger.name:
#     res = make_vitrine_01_02()
# # print (res)

# %%

# %% [markdown]
# ## вар2: ПредВитрина №№01-02
# сначала собираем все нужные данные в вертикальную таблицу, потом делаем сводную таблицу - горизонтальную широкую витрину

# %% tags=[]
def make_vitrine_01_02_plus(vitrine_name='vitrine_03'):
    """
        -- Промежуточный результат: Витрина с данными задачи №01 и №02
        -- выполнение одним запросом без сохранения промежуточных таблиц
    """
    # TODO: надо бы еще формирование списков источников сделать динамическим
    #
    q = f"""
        DROP TABLE IF EXISTS data_00_01;
        WITH 
         cat_list AS (SELECT cat_group AS "Категория" FROM {MAIN_TABLE_NAME}_cat GROUP BY "Категория" ORDER BY "Категория")-- список категорий
        ,cat_nlist AS(SELECT ROW_NUMBER() OVER() AS "N", "Категория"   FROM cat_list) -- нумерованный список категорий
        ,all_src AS  (SELECT cat_group AS "Категория", '0Все источники' AS "Источник", count(*) AS "Всего новостей"
                      FROM {MAIN_TABLE_NAME}_cat GROUP BY cat_group ORDER BY cat_group)
        ,each_src AS (SELECT cat_group AS "Категория", "source" as "Источник", count(*) AS "Всего новостей"
                      FROM {MAIN_TABLE_NAME}_cat GROUP BY "Категория", "Источник" ORDER BY "Категория", "Источник")
        SELECT * 
        INTO data_00_01
        FROM all_src
        UNION
        SELECT * FROM each_src
        ORDER BY "Категория","Источник";
        -------------------------------------------------------------------------
        -- из сохранненой таблицы с объединенными данными делаем сводную таблицу
        DROP TABLE IF EXISTS {vitrine_name};
        SELECT * 
        INTO {vitrine_name}
        FROM 
        crosstab($$SELECT * FROM data_00_01 $$
                ,$$SELECT DISTINCT "Источник" FROM data_00_01 ORDER BY "Источник" $$
        ) AS ct ("Категория_02" TEXT
                ,"Все источники" INT8
                ,"habr.com|ru|rss|all|all|" INT8
                ,"hibinform.ru|feed|" INT8
                ,"lenta.ru|rss|" INT8
                ,"regnum.ru|rss" INT8
                ,"ria.ru|export|rss2|archive|index.xml" INT8
                ,"rossaprimavera.ru|rss" INT8
                ,"tass.ru|rss|v2.xml" INT8
                ,"www.cnews.ru|inc|rss|news.xml" INT8
                ,"www.kommersant.ru|RSS|news.xml" INT8
                ,"www.vedomosti.ru|rss|news" INT8
                );
        COMMIT;
     """
    
    
    res = SQL_ENGINE.execute(q)
    # conn = SQL_ENGINE.connect()
    # res = conn.execute(q)
    # res = conn.execute("""SELECT * FROM vitrine_03""")
    # conn.commit_prepared()
    # conn.close()
    
    # logger.debug(f'суппер-пуупер запрос: {res.rowcount}')

   
    logger.debug(f'Сформирована витрина ({vitrine_name}) по п.1 и п.2. Всего записей: {res.rowcount}')
    # logger.debug(f'записей: {res.all()}')
    
    return res
    
# #тест
# res = ''
# if "DEBUG" in logger.name:
#     res = make_vitrine_01_02()
# # print (res)

# %% [markdown]
# # ** **CRON**: Формирование главной витрины

# %% tags=[]
def cron_vitrine():
    """ последовательность действий для формирования финальной витрины
        
    """
    # формирование объединеной таблицы с данными по всем источникам
    make_union_main_table()
    logger.info('Объединенная таблица с данными сформирована.')
    
    # (костыли) загрузка в таблицу сформированного вручную файла с группами категрий
    load_category_map_from_file()
    logger.info('Группы категорий загружены.')
    
    
    # добавление групп категорий к главной таблице с объединенными данными по всем источникам
    add_cat_group_to_main_table()
    logger.info('Данным присвоены группы категорий.')
    
    # формирование витрины для первых двух заданий: 
    # 01 : КАЖДАЯ категория, ВСЕ источники, ВСЕ дни
    # 02 : КАЖДАЯ категория, КАЖДЫЙ источник, ВСЕ дни 
    make_vitrine_01_02_plus()
    logger.info('Витрина сформирована.')
    
    
# #тест
# res = ''
# if "DEBUG" in logger.name:
#     cron_vitrine()
# # print (res)
    
