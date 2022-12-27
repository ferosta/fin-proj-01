#!/usr/bin/env python
# coding: utf-8

# In[1]:


from rss_parser import Parser
from requests import get
import pandas as pd
import os 
import json
import time
from datetime import datetime, timezone


# # Конфигурационные настройки

# In[2]:


# логирование
# PRJ_DIR = "" #'/home/fedorov/mypy/vk_prj/'
# if PRJ_DIR not in sys.path:
#     sys.path.insert(0, PRJ_DIR)
##########################################
# логирование
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
            "handlers":["fileHandlerINFO", "GlobalfileHandler"],
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
# logger = logging.getLogger("INFO."+PROG_NAME)
# logger = logging.getLogger("INFO."+PROG_NAME)
logger = logging.getLogger("DEBUG."+PROG_NAME)


# In[3]:


# конфигурационные настройки
config_file_name = os.path.abspath(u'./config/rss_links.csv')
data_dir_name = os.path.abspath(u'./data')


# # Чтение конфига с адресами источников РСС

# In[4]:


# читаем конфиг со ссылками на источники
def read_config(config_file_name):
    """читаем конфиг со ссылками на источники
        config_file_name - имя файла с конфигом (если не в локальной директории то с путём)
    """
    df_config = pd.read_csv(config_file_name, header=None  )
    rss_urls = list(df_config[0])
    logger.debug(f'Ссылки на источники прочитаны из {config_file_name}')
    return rss_urls


# rss_urls = read_config(config_file_name)
# rss_urls


# # Подготовка первичного хранилища для данных из источников

# In[5]:


# подготовить: проверить и если надо создать каталог под данные из источника
def rss_dir_prepare(rss_url):
    """ Проверить есть ли каталог для данного источника,
        Если нет, то создать каталог для сохранения сведений из источника .
        rss_url - ссылка на источник из конфиг-файла
    """
    # получаем имя папки с данными из ссылки на источник
    rss_dir_name = rss_url.replace(u'https://', "").replace(u"/","|")
    logger.debug(f'Проверяется папка rss_dir_name = {rss_dir_name}')
    
    # полный путь до папки с данными
    rss_full_dir_name = os.path.join(data_dir_name , rss_dir_name ) 
    rss_abs_dir_name =  os.path.abspath(rss_full_dir_name)
    
    # если такой папки еще нет - то создаем
    if not os.path.exists(rss_abs_dir_name):
        os.mkdir(rss_abs_dir_name)
        logger.debug(f'Создна папка {rss_abs_dir_name}')
    
    return rss_abs_dir_name

    
# rss_url = 'https://lenta.ru/rss/' # rss_urls[0]
# rss_dirname = rss_dir_prepare(rss_url)


# # Получение данных из источника по ссылке 

# In[6]:


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

# rss_url = 'https://lenta.ru/rss/' # rss_urls[0]
# rss_feed = get_rss(rss_url)


# # Сохранение полученных из истончика данных в файл

# In[7]:


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

        
# rss_filename = save_rss_feed(rss_feed, rss_dirname)


# # Тест 1: один источник

# In[8]:


def get_one_rss_data():
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config(config_file_name)

    # подготавливаем папки для хранения скачиваемых из РСС данных
    rss_url = 'https://lenta.ru/rss/' # rss_urls[0]
    rss_dirname = rss_dir_prepare(rss_url)

    # получаем порцию данных по ссылке
    # rss_url = 'https://lenta.ru/rss/' # rss_urls[0]
    rss_feed = get_rss(rss_url)

    # сохраняем данные в заранее подготовленной папке
    rss_filename = save_rss_feed(rss_feed, rss_dirname)


# # Тест 2: циклический перебор всех источников RSS

# In[9]:


def get_all_rss_data():
    """ Получение данных из всех источников """
    # читаем конфиг с адресами источников РСС
    rss_urls = read_config(config_file_name)

    for url in rss_urls:

        # подготавливаем папки для хранения скачиваемых из РСС данных
        dirname = rss_dir_prepare(url)

        # получаем порцию данных по ссылке
        feed = get_rss(url)

        # сохраняем данные в заранее подготовленной папке
        rez_filename = save_rss_feed(feed, dirname)


# # Загрузка данных из файлов в хранилище (БД)

# In[ ]:




