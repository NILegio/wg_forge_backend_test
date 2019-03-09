import psycopg2, os, json
from psycopg2.extras import RealDictCursor
from flask import Flask, jsonify, request


THIS_FOLDER = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILES = os.path.join(THIS_FOLDER, 'config.json')
app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


def get_config():
    with open(CONFIG_FILES, 'rt') as file:
        return json.load(file)


def get_name_list():
    with psycopg2.connect(**conf) as conn:
        with conn.cursor() as curs:
            curs.execute("SELECT name FROM cats")
            name_list = []
            for param in curs:
                name_list.append(param[0])
            return name_list


def exception(data, name_list, method):
    errors = []
    db_columns = ('name', 'color', 'tail_length', 'whiskers_length')
    if method == 'POST':
        for column in db_columns:
            if column not in data:
                errors.append('There is no {} in your POST request'.format(column))
    for k, v in data.items():
        if k in ('limit', 'offset', 'tail_length', 'whiskers_length'):
            try:
                if int(v) < 0:
                    errors.append('{0}={1} cannot be less than zero'.format(k, v))
            except ValueError:
                errors.append('{0}={1} is not a number'.format(k, v))
                continue
        if method == 'GET':
            if k not in ('attribute', 'order', 'limit', 'offset'):
                errors.append('Invalid GET parameter {0}={1} in URL'.format(k, v))
            elif k == 'order':
                if 'attribute' not in data:
                    errors.append('You must use parameter \'attribute\' and \'order\' in same GET-request')
                if v not in ('asc', 'desc'):
                    errors.append('Keyword {} is invalid for statement ORDER BY. Please use ASC of DESC'.format(v))
            elif k == 'offset':
                if int(v) > len(name_list):
                    errors.append('Parameter OFFSET is more than values in Database')
            elif k == 'attribute':
                if 'order' not in data:
                    errors.append('You must use parameter \'attribute\' and \'order\' in same GET-request')
                if v not in ('name', 'color', 'tail_length', 'whiskers_length'):
                    errors.append('Invalid parameter {0}={1} for database'.format(k, v))
        if method == 'POST':
            if k == 'name':
                if v in name_list:
                    errors.append('Name {} is already in database'.format(v))
            if k == 'color':
                if v not in ('black', 'white', 'black & white', 'red', 'red & white', 'red & black & white'):
                    errors.append('Invalid color {} for database'.format(v))
            if k in ('tail_length', 'whiskers_length'):
                if v > 35:
                    errors.append('{0} {1} is too big'.format(*k.split('_')))
    return errors


@app.route('/ping')
def index():
    return "Cats Service. Version 0.1"


@app.route('/cats', methods=['GET'])
def get_cats():
    order_cats = ''
    limit_cats = ''
    offset_cats = ''
    raw_data = request.args
    name_list = get_name_list()
    method = 'GET'
    errors = exception(raw_data, name_list, method)
    if errors:
        return jsonify(errors)
    for k in raw_data.keys():
        try:
            if k == 'attribute':
                order_cats = 'ORDER by {attribute} {order}'\
                    .format(order=raw_data['order'],  attribute=raw_data['attribute'])
            if k == 'limit':
                limit_cats = 'LIMIT {limit}'.format(limit=raw_data['limit'])
            if k == 'offset':
                offset_cats = 'OFFSET {offset}'.format(offset=raw_data['offset'])
        except KeyError:
            continue
    query_cats = "SELECT * FROM cats {0} {1} {2} ".format(order_cats, limit_cats, offset_cats)
    with psycopg2.connect(**conf) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as curs:
            curs.execute(query_cats)
            res = [dict(record) for record in curs]
    return jsonify(res)


@app.route('/cat', methods=['POST'])
def post_cat():
    raw_data = request.get_json(force=True)
    name_list = get_name_list()
    method = 'POST'
    errors = exception(raw_data, name_list, method)
    if errors:
        return jsonify(errors)
    with psycopg2.connect(**conf) as conn:
        with conn.cursor() as curs:
            curs.execute("INSERT INTO cats VALUES (%(name)s, %(color)s, %(tail_length)s, %(whiskers_length)s)",
                         {'name': raw_data['name'], 'color': raw_data['color'], 'tail_length': raw_data['tail_length'],
                          'whiskers_length': raw_data['whiskers_length']})
        conn.commit()
    return jsonify('New cat have arrived in database')


if __name__ == '__main__':
    conf = get_config()
    app.run(debug=True, port=8080)
