import psycopg2
import json
import os
from psycopg2.extras import DictCursor


THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILES = os.path.join(THIS_FOLDER, 'config.json')


def get_config():
    with open(CONFIG_FILES, 'rt') as file:
        return json.load(file)  # (dbname='wg_forge_db', user='postgres', password='vi7al0')


def count_color():
    with psycopg2.connect(**conf) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT DISTINCT color FROM cats")
            example_colors = curs.fetchall()
        for i in range(len(example_colors)):
            with conn.cursor() as curs:
                curs.execute("SELECT color FROM cats WHERE color = %s", (example_colors[i][0],))
                list_color = curs.fetchall()
            counted_color = len(list_color)
            with conn.cursor() as curs:
                curs.execute("INSERT INTO cat_colors_info VALUES (%s, %s)",
                             (example_colors[i][0], counted_color))
        conn.commit()


def get_data():
    with psycopg2.connect(**conf) as conn:
        with conn.cursor(cursor_factory=DictCursor) as curs:
            curs.execute("SELECT tail_length, whiskers_length FROM cats")
            data = {'tail_length': [], 'whiskers_length': []}
            for row in curs:
                for k in row.keys():
                    data[k].append(row[k])
            return data


def costume_mean(data):
    return round(sum(data)/len(data), 1)


def costume_median(raw_data):
    data = raw_data[:]
    data.sort()
    raw_middle = len(data)//2
    if len(data) % 2 == 0:
        middle = int(raw_middle)
        return (data[middle]+data[middle-1])/2
    else:
        middle = int(raw_middle)
        return data[middle]


def costume_mode(data):
    raw_mode = {}
    for i in data:
        if raw_mode.get(i):
            raw_mode[i] += 1
        else:
            raw_mode[i] = 1
    max_value = 0
    mode = []
    for v in raw_mode.values():
        if max_value < v:
            max_value = v
    for k, v in raw_mode.items():
        if v == max_value:
            mode.append(k)
    mode.append(max_value)
    return mode


def cat_characteristics():
    data = get_data()
    cat_data = []
    for v in data.values():
        cat_data.append(costume_mean(v))
        cat_data.append(costume_median(v))
        cat_data.append(costume_mode(v))
    with psycopg2.connect(**conf) as conn:
        with conn.cursor() as curs:
            curs.execute("INSERT INTO cats_stat VALUES (%s, %s, %s, %s, %s, %s)", cat_data)
        conn.commit()


if __name__ == "__main__":
    conf = get_config()
    cat_characteristics()
    count_color()
