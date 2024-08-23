from yaml import safe_load
import logging

with open('./configs_queries.yaml', "r", encoding="utf-8") as file:
    configs = safe_load(file)

queue_raw = configs['queries']['select_result']


class ObjFetch:
    def __init__(self, fetch: tuple, index: int, des=True, width=40):
        self.description = ''
        self.des = des
        self.year, self.genre, self.name, self.width = None, None, None, width
        self.fetch = fetch
        self.index: int = index
        self.unpack()
        self.list_lines = self.create_self_list_lines()
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def create_dict(cls, tuples):
        i = 1
        for tuple1 in tuples:
            i += 1
            ObjFetch(tuple1, i)

    def unpack(self):
        if self.fetch:
            if len(self.fetch) == 1:
                self.genre = self.fetch[0]
            else:
                self.name, self.genre, self.year, self.description = self.fetch

    def create_self_list_lines(self, des=False) -> list:
        result_list = []
        if self.genre and self.name is None and self.year is None:
            lines_list_raw = [f'Номер жанра: {self.index}',
                              f'Жанр: {self.genre}']
        else:
            lines_list_raw = [f'Номер фильма: {self.index}',
                              f'Название : {self.name}',
                              f'Год выпуска: {self.year}, '
                              f'жанр: {self.genre}']
        if des:
            lines_list_raw.append(f'Описание: {self.description}')
        for element in lines_list_raw:
            list_words = element.split()
            if len(element) < self.width:
                result_list.append(element)
            else:
                line = ''
                for word in list_words:
                    if len(word) + len(line) < self.width:
                        line += f'{word} '
                    else:
                        result_list.append(line)
                        line = ''
        return result_list

    def __str__(self):
        str_list_lines = self.create_self_list_lines(True)
        if str_list_lines:
            line_str = ''
            for element in str_list_lines:
                line_str += f'{element}\n'
            return line_str


class Pages:

    @classmethod
    def parse_list_obj_to_column_page(cls, obj_list):
        cls.dict_pages = {}
        list_lines = []
        count_raw = 1
        count_page = 1
        try:
            if obj_list:
                for i in range(0, len(obj_list), 2):
                    if i + 1 < len(obj_list):
                        list_lines.extend(cls.column(obj_list[i], obj_list[i + 1]))
                    else:
                        list_lines.extend(cls.column(obj_list[i]))
                    if list_lines:
                        count_raw += 1
                        cls.dict_pages[count_page] = list_lines
                        if count_raw == 11:
                            count_page += 1
                            list_lines = []
                            count_raw = 1
            count_page = list(cls.dict_pages.keys())[-1]
            for _ in cls.dict_pages[1]:
                print(_)
            while True:
                if count_page == 1:
                    break
                try:
                    inp = int(input(f"""Введите номер страницы из {count_page} или "0" для выхода """).strip())
                except ValueError:
                    pass
                    continue
                if inp == 0:
                    break
                if inp in cls.dict_pages:
                    for _ in cls.dict_pages[inp]:
                        print(_)
        except Exception as e:
            pass

    @classmethod
    def column(cls, obj1, obj2=None):
        result_column_lines_list = []
        column1 = obj1.list_lines
        column2 = obj2.list_lines if obj2 else ['']
        max_length = max(len(column1), len(column2))
        for i in range(max_length):
            line1 = column1[i] if i < len(column1) else ''
            line2 = column2[i] if i < len(column2) else ''
            formatted_line = f"{line1.ljust(50)} {line2}"
            # print(formatted_line)
            result_column_lines_list.append(formatted_line)
        # print('')
        result_column_lines_list.extend([''])
        return result_column_lines_list

    # def column(cls, obj1, obj2):
    #     result_column_lines_list = []
    #     column1 = obj1.list_lines
    #     column2 = obj2.list_lines if obj2 else ['']
    #     if column1 or column2:
    #         for i in range(max(len(column1), len(column2))):
    #             line1 = column1[i] if i < len(column1) else ''
    #             line2 = column2[i] if i < len(column2) else ''
    #             print(f'{line1.ljust(50, ' ')} {line2}')
    #             result_column_lines_list.append(f'{line1.ljust(50)} {line2}')
    #         print('')
    #         result_column_lines_list.append('')
    #         result_column_lines_list.append('')
    #     return result_column_lines_list
