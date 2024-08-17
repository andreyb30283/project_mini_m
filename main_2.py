from connector import DatabaseConnector as Dc
from connector import db_config_read, db_config_write
import FilmQueryCLI_V2
import yaml
import logging

with open('./configs_queries.yaml', "r", encoding="utf-8") as file:
    configs = yaml.safe_load(file)
    print(configs)


class MenuChoice:
    def __init__(self):
        self.search_index = None
        self.year_start = 0
        self.year_end = 3000
        self.test_result_db = []
        self.genre = ''
        self.search_key = ''
        self.year_start_inp = ''
        self.year_end_inp = ''
        self.genre_inp = ''
        self.choice = ''
        self.film_name_to_db = ''
        self.dict_genre = {}
        self.logger = logging.getLogger(__name__)
        self.queue_read_from_rw_db = configs['queries']['database_write']['read_from_table']
        logging.basicConfig(level=logging.INFO)

    @staticmethod
    def green_t(text):
        return f'\033[32m{text}\033[0m'

    def main_menu(self):
        print(f'{"_" * 40}\nГлавное меню:')
        print(f"1. Фильтрация фильмов по названию {self.green_t(self.film_name_to_db)}")
        print(f"2. Фильтрация по жанру {self.green_t(self.genre)}")
        print(f"3. Фильтрация по году {self.green_t(self.year_start_inp)}-{self.green_t(self.year_end_inp)} ")
        print("4. Поиск по базе\n5. Просмотр топ запросов ")
        self.choice = input("Выберите пункт меню (введите цифру): ")
        if self.choice == '1':
            self.filter_by_name()
        elif self.choice == '2':
            self.filter_by_genre()
        elif self.choice == '3':
            self.filter_by_year()
        elif self.choice == '4':
            self.search()
        elif self.choice == '5':
            self.top_search()
        else:
            self.main_menu()

    def filter_by_name(self):
        print('_______________________________________________________________________________')
        self.film_name_to_db = input('Введите название фильма: ').strip()
        self.main_menu()

    def filter_by_genre(self):
        print('_______________________________________________________________________________')
        list_genre = Dc.rw_to_db(configs['queries']['select_genre'], None, db_config_read)
        list_obj = []
        for i, name in enumerate(list_genre):
            i += 1
            list_obj.append(FilmQueryCLI_V2.ObjFetch(name, i))
            self.dict_genre[i] = name[0].strip()
        FilmQueryCLI_V2.Pages.parse_list_obj_to_column_page(list_obj)
        self.genre_inp = input('Введите жанр или его номер: ').strip()
        if self.genre_inp == '':
            self.genre = ''
        else:
            for key, genre in self.dict_genre.items():
                if self.genre_inp.isdigit() and int(self.genre_inp) == int(key):
                    self.genre = genre
                elif self.genre_inp.lower() in f'{genre}'.lower():
                    self.genre = genre
            if self.genre == '':
                print(f"{self.green_t(f"Жанр '{self.genre_inp}' не найден.")}")
        self.main_menu()

    def top_search(self):
        print('_______________________________________________________________________________')
        print('ТОП ЗАПРОСОВ ДЛЯ ПОИСКА ФИЛЬМОВ')
        try:
            top_s_res = Dc.rw_to_db(self.queue_read_from_rw_db, None, db_config_write)
            if top_s_res:
                for i, value in enumerate(top_s_res):
                    i += 1
                    value1, value2 = value
                    value1 = value1.strip()
                    print(f"частота запроса '{self.green_t(value2)}' запрос: '{self.green_t(value1)}' ")
        except Exception as e:
            print(f'top_search: {e}')
        input(f'Нажмите {self.green_t(' "ENTER" ')} для выхода из просмотра топ запросов. ')
        self.main_menu()

    def filter_by_year(self):
        print('_______________________________________________________________________________')
        try:
            self.year_start_inp = int(input('Введите с какого года фильм : ').strip())
            self.year_end_inp = int(input('Введите до какого года фильм : ').strip())

        except ValueError:
            print("Введите число (Например: 2000)")
            self.year_start_inp = self.year_end_inp = ''

        self.main_menu()

    @staticmethod
    def db_fetch_print(list_films):
        if list_films:
            list_obj = []
            for i, name in enumerate(list_films):
                i += 1
                list_obj.append(FilmQueryCLI_V2.ObjFetch(name, i))
                # self.dict_genre[i] = name[0].strip()
            FilmQueryCLI_V2.Pages.parse_list_obj_to_column_page(list_obj)

    # def search(self):
    #     print('_______________________________________________________________________________')
    #                                 # print(f'Ключевое слово для поиска "{self.film_name_to_db}":  ')
    #                                 # self.search_key = input(f'Введи ключевое слово для поиска по имени :')
    #                                 #
    #                                 # if self.search_key:
    #                                 #     self.film_name_to_db = self.search_key
    #     if self.year_end_inp:
    #         self.year_end = self.year_end_inp
    #     else:
    #         self.year_end = 3000
    #     if self.year_start_inp:
    #         self.year_start = self.year_start_inp
    #     else:
    #         self.year_start = 0
    #     params = None
    #     # dict_search = {}
    #     queue_raw = configs['queries']['select_result']
    #     queue_read = queue_raw + f" and f.release_year>={self.year_start} and f.release_year<={self.year_end}"
    #     if self.genre:
    #         queue_read += f" and c.name like '{self.genre}'"
    #     if self.film_name_to_db:
    #         queue_read += " and f.title like %s"
    #         params = ('%' + self.film_name_to_db + "%",)
    #     queue_read += " order by f.title "
    #     self.logger.info(f'{queue_read} {params}')
    #     list_films = Dc.rw_to_db(queue_read, params, db_config_read)
    #
    #
    #
    #     self.query_to_db()
    #     if list_films:
    #         if len(list_films) == 1:
    #             name = list_films
    #             print(
    #                 f"Название '{self.green_t(name[0])}' жанр '{self.green_t(name[1])}'. "
    #                 f"год выпуска фильма '{self.green_t(name[2])}' ")
    #             print(f'Описание фильма: {self.green_t(name[-1])}')
    #         list_obj = []
    #         for i, name in enumerate(list_films):
    #             i += 1
    #             list_obj.append(FilmQueryCLI_V2.ObjFetch(name, i))
    #         FilmQueryCLI_V2.Pages.parse_list_obj_to_column_page(list_obj)
    #         print(f'Введите номер или часть имени фильма или "0" для выхода из этого меню.')
    #         self.search_key = input(f'Ключевое слово для поиска"{self.search_key}": ')
    #         if self.search_key == '0':
    #             self.main_menu()
    #         elif self.search_key.isdigit():
    #             self.search_index: int = int(self.search_key)
    #             if self.search_index - 1 in range(0, len(list_obj)):
    #                 print(list_obj[self.search_index - 1])
    #                 self.search()
    #         else:
    #             self.film_name_to_db = self.search_key
    #
    #         self.search()
    #     else:
    #         print('Кино не найдено')
    #         self.film_name_to_db = ''
    #         self.search()

    def search(self):

        print('_______________________________________________________________________________')

        def request_to_db():
            # Установка значений для года начала и конца
            self.year_end = self.year_end_inp if self.year_end_inp else 3000
            self.year_start = self.year_start_inp if self.year_start_inp else 0
            # Формирование запроса к базе данных
            params = None
            query = configs['queries']['select_result']
            query += f" and f.release_year >= {self.year_start} and f.release_year <= {self.year_end}"
            if self.genre:
                query += f" and c.name like '{self.genre}'"
            if self.film_name_to_db:
                query += " and f.title like %s"
                params = ('%' + self.film_name_to_db + "%",)
            query += " order by f.title"
            self.logger.info(f'{query} {params}')
            list_films1 = Dc.rw_to_db(query, params, db_config_read)
            return list_films1

        list_films = request_to_db()
        self.query_to_db()
        if list_films:
            # if len(list_films) == 1:
            #     name = list_films[0]
            #     list_obj = FilmQueryCLI_V2.ObjFetch(name, 1, True)
            #     FilmQueryCLI_V2.Pages.parse_list_obj_to_column_page(list_obj)
            #     # print(f"Название '{self.green_t(name[0])}' жанр '{self.green_t(name[1])}'. "
            #     #       f"год выпуска фильма '{self.green_t(name[2])}' ")
            #     # print(f'Описание фильма: {self.green_t(name[-1])}')
            # else:
            list_obj = [FilmQueryCLI_V2.ObjFetch(name, i + 1) for i, name in enumerate(list_films)]
            FilmQueryCLI_V2.Pages.parse_list_obj_to_column_page(list_obj)
            print(f'Введите номер или часть имени фильма или "0" для выхода из этого меню.')
            self.search_key = input(f'Ключевое слово для поиска "{self.search_key}": ')
            if self.search_key == '0':
                self.main_menu()
            elif self.search_key.isdigit():
                self.search_index = int(self.search_key)
                if 0 <= self.search_index - 1 < len(list_obj):
                    self.film_name_to_db = list_obj[self.search_index - 1].name
                    # print(list_obj[self.search_index - 1].name)
                    one_film = request_to_db()
                    one_film = one_film[0]
                    one_obj = FilmQueryCLI_V2.ObjFetch(one_film, 1)
                    print(one_obj)
                    # list_one_obj=[]
                    # list_one_obj.append(one_obj)
                    # FilmQueryCLI_V2.Pages.parse_list_obj_to_column_page(list_one_obj)

                    # print(f"Название '{one_film[0]}' жанр '{one_film[1]}'. "
                    #       f"год выпуска фильма '{one_film[2]}' ")
                    # print(f'Описание фильма: {one_film[-1]}')
                    # input("Нажмите ентер чтобы продолжить")
                    self.search_key = ''
                    self.film_name_to_db = ''
                    self.search()
            else:
                self.film_name_to_db = self.search_key
                self.search()
        else:
            print('Кино не найдено')
            self.film_name_to_db = ''
            self.search()

    # for key, value in dict_search.items():
    #     if self.genre_inp.lower() in f'{key}{value}'.lower():
    #         self.genre_inp = value

    def query_to_db(self):
        print('_______________________________________________________________________________')
        try:
            create_table_queue = configs['queries']['database_write']['create_table']
            write_in_table_new_count = configs['queries']['database_write']['write_in_table_new_count']
            write_in_table_new_queue = configs['queries']['database_write']['write_in_table_new_queue']
            Dc.rw_to_db(create_table_queue, None, db_config_write)
            self.logger.info('query_to_db OK')

            data_queue_to_rw_db = f'{self.genre} {self.year_start_inp}-{self.year_end_inp} {self.film_name_to_db}'
            list_queries = Dc.rw_to_db(self.queue_read_from_rw_db, None, db_config_write)

            # data_queue_to_rw_db = 'New 0-3000+'
            # list_queries = [('test', 3), ('New 0-3000+', 1)]
            self.logger.info(f"list_queries: {list_queries} data_queue_to_rw_db: {data_queue_to_rw_db}")
            if self.genre or self.film_name_to_db:
                for queue, count in list_queries:
                    # print(queue)
                    # print(data_queue_to_rw_db)

                    if queue == data_queue_to_rw_db:
                        count += 1
                        try:
                            self.test_result_db = Dc.rw_to_db(
                                write_in_table_new_count,
                                (count, data_queue_to_rw_db),
                                db_config_write)
                        except Exception as error:
                            self.logger.error(f"Ошибка в query_to_db: {error}")
                            raise

                        break
                else:
                    try:
                        self.test_result_db = Dc.rw_to_db(write_in_table_new_queue,
                                                          (data_queue_to_rw_db,),
                                                          db_config_write)
                    except Exception as error:
                        self.logger.error(f"Ошибка в query_to_db: {error}")
                        raise
            self.logger.info(f'self.test_result_db : {self.test_result_db}')
            # dict_query = {}
            # if list_queries:
            #     for i, name in enumerate(list_queries):
            #         i += 1
            #         # for n in name:
            #         print(f'{i} {name}')
            #         dict_query[i] = name

        except Exception as error:
            self.logger.info(f"{error} error query_to_db")


menu = MenuChoice()
menu.main_menu()
