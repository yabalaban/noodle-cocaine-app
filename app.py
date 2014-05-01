from excepts import InvalidUsage
from flask import Flask, request, jsonify
from starbase import Connection


connection = Connection(host='85.10.254.212', port='20550')
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello World!'


@app.route('/stream')
def stream():
    if request.content_type == 'application/json':
        inp = request.get_json()
        table = inp['collection']
        t = connection.table(table)
        if not t.exists():
            raise InvalidUsage('Illegal data structure')
        else:
            try:
                batch = t.batch()
                for row in inp['data']:
                    key = row['id']
                    values = row['values']
                    batch.insert(key, values)
                batch.commit()
            except Exception as e:
                raise InvalidUsage(e.message)
            return "Success"
    else:
        raise InvalidUsage('Only "application/json" content type is allowed', status_code=415)


@app.errorhandler(InvalidUsage)
def handle_invalid_usage(error):
    response = jsonify(error.to_dict())
    response.status_code = error.status_code
    return response


if __name__ == '__main__':
    app.run()
