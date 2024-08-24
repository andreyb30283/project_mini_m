from connector import DatabaseConnector as Dc
import Film_pages
import yaml


with open('./configs_queries.yaml', "r", encoding="utf-8") as file:
    configs = yaml.safe_load(file)

db_config_read = configs['data_bases']['database_read']
db_config_write = configs['data_bases']['database_write']
read_from_rw_db_limit = configs['queries']['database_write']['read_from_table_lim']
create_table_queue = configs['queries']['database_write']['create_table']
write_in_table_new_count = configs['queries']['database_write']['write_in_table_new_count']
write_in_table_new_queue = configs['queries']['database_write']['write_in_table_new_queue']
query_read_raw = configs['queries']['select_result']
query_read_genres = configs['queries']['select_genre']
queue_read_from_rw_db = configs['queries']['database_write']['read_from_table']

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
        list_genre = Dc.rw_to_db(query_read_genres, None, db_config_read)
        list_obj = []
        for i, name in enumerate(list_genre):
            i += 1
            list_obj.append(Film_pages.ObjFetch(name, i))
            self.dict_genre[i] = name[0].strip()
        Film_pages.Pages.parse_list_obj_to_column_page(list_obj)
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
            top_s_res = Dc.rw_to_db(read_from_rw_db_limit, None, db_config_write)
            if top_s_res:
                for i, value in enumerate(top_s_res):
                    i += 1
                    queue, count = value
                    queue = queue.strip()
                    print(f"частота запроса '{self.green_t(count)}' запрос: '{self.green_t(queue)}' ")
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
                list_obj.append(Film_pages.ObjFetch(name, i))
            Film_pages.Pages.parse_list_obj_to_column_page(list_obj)

    def search(self):
        print('_______________________________________________________________________________')

        def request_to_db():
            query_tmp = ''
            self.year_end = self.year_end_inp if self.year_end_inp else 3000
            self.year_start = self.year_start_inp if self.year_start_inp else 0
            params = None
            if self.year_start or self.year_end:
                query_tmp = query_read_raw + (f" and f.release_year >="
                                              f" {self.year_start} and f.release_year <= {self.year_end}")
            if self.genre:
                query_tmp += f" and c.name like '{self.genre}'"
            if self.film_name_to_db:
                query_tmp += " and f.title like %s"
                params = ('%' + self.film_name_to_db + "%",)
            query_tmp += " order by f.title "
            return Dc.rw_to_db(query_tmp, params, db_config_read)

        list_films = request_to_db()
        if len(list_films) == 1:
            one_obj = Film_pages.ObjFetch(list_films[0], self.search_index if self.search_index else 1)
            print(one_obj)
            self.search_key = ''
            self.film_name_to_db = ''
            input(f"{self.green_t("Press Enter")}")
            self.search()
        if list_films:
            list_obj = [Film_pages.ObjFetch(name, i + 1) for i, name in enumerate(list_films)]
            Film_pages.Pages.parse_list_obj_to_column_page(list_obj)
            print(f'Введите номер или часть имени фильма или "0" для выхода из этого меню.')
            self.search_key = input(f'Ключевое слово для поиска "{self.search_key}": ').strip()
            if self.search_key == '0':
                self.main_menu()
            elif self.search_key.isdigit():
                self.search_index = int(self.search_key)
                if 0 <= self.search_index - 1 < len(list_obj):
                    self.film_name_to_db = list_obj[self.search_index - 1].name
                    self.query_to_db()
                    self.search()
            else:
                self.film_name_to_db = self.search_key
                self.query_to_db()
                self.search()
        else:
            self.film_name_to_db = self.search_key
            print('Кино не найдено')
            input("Press Enter ")
            self.film_name_to_db = ''
            self.query_to_db()
            self.main_menu()

    def query_to_db(self):
        print('_______________________________________________________________________________')
        try:
            Dc.rw_to_db(create_table_queue, None, db_config_write)
            data_queue_to_rw_db = ''
            data_queue_to_rw_db += f'Жанр: {self.genre} ' if self.genre else ''
            data_queue_to_rw_db += f' Год выпуска: {self.year_start_inp} ' if self.year_start_inp else ''
            data_queue_to_rw_db += f'- {self.year_end_inp} ' if self.year_end_inp else ''
            data_queue_to_rw_db += f'Ключевое слово: "{self.film_name_to_db}"' if {self.film_name_to_db} else ''
            list_queries = Dc.rw_to_db(queue_read_from_rw_db, None, db_config_write)
            if data_queue_to_rw_db:
                if list_queries:
                    for queue, count in list_queries:
                        if queue == data_queue_to_rw_db:
                            count += 1
                            try:
                                Dc.rw_to_db(write_in_table_new_count, (count, data_queue_to_rw_db), db_config_write)
                            except Exception as error:
                                print(f"Ошибка try update COUNT в query_to_db: {error}")
                                raise
                            break
                    else:
                        try:
                            Dc.rw_to_db(write_in_table_new_queue, (data_queue_to_rw_db,), db_config_write)
                        except Exception as error:
                            print(f"Ошибка else try write в query_to_db: {error}")
                            raise

                else:
                    try:
                        Dc.rw_to_db(write_in_table_new_queue, (data_queue_to_rw_db,), db_config_write)
                    except Exception as error:
                        print(f"Ошибка в query_to_db: {error}")
                        raise

        except Exception as error:
            print(f"query_to_db {error}")


menu = MenuChoice()
menu.main_menu()
