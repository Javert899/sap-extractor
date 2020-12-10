from flask import Flask, render_template, request, make_response, jsonify, redirect, url_for, send_file
import base64
import json
from sapextractor import factory
import tempfile
from flask_cors import CORS


app = Flask(__name__)
CORS(app, expose_headers=["x-suggested-filename"])


@app.route('/')
def empty_path():
    return redirect(url_for('welcome'))


@app.route('/index.html')
def index():
    return redirect(url_for('welcome'))


@app.route("/welcome")
def welcome():
    response = make_response(render_template('extraction.html'))
    return response


@app.route("/downloadLog")
def download_event_log():
    parameters = request.args.get("parameters")

    try:
        parameters = json.loads(base64.b64decode(parameters))
    except:
        parameters = {}

    db_type = parameters["db_type"] if "db_type" in parameters else "sqlite"
    db_con_args = parameters["db_con_args"] if "db_con_args" in parameters else {"path": "sap.sqlite"}
    process = parameters["process"] if "process" in parameters else "ap_ar"
    ext_type = parameters["ext_type"] if "ext_type" in parameters else "document_flow_log"
    ext_arg = parameters["ext_arg"] if "ext_arg" in parameters else {}

    log = factory.apply(db_type, db_con_args, process, ext_type, ext_arg)

    if "obj_centr" in ext_type:
        extension = ".ocelxml"
        temp_file = tempfile.NamedTemporaryFile(suffix=extension)
        temp_file.close()
        from pm4pymdl.objects.ocel.exporter import exporter as ocel_exporter
        ocel_exporter.apply(log, temp_file.name)
    elif "dataframe" in ext_type:
        extension = ".csv"
        temp_file = tempfile.NamedTemporaryFile(suffix=extension)
        temp_file.close()
        log.to_csv(temp_file.name, index=False)
    elif "log" in ext_type:
        extension = ".xes"
        temp_file = tempfile.NamedTemporaryFile(suffix=extension)
        temp_file.close()
        from pm4py.objects.log.exporter.xes import exporter as xes_exporter
        xes_exporter.apply(log, temp_file.name)
    resp = send_file(temp_file.name,
                       mimetype="text/plain",  # use appropriate type based on file
                       as_attachment=True,
                       conditional=False)
    resp.headers["x-suggested-filename"] = "log" + extension

    return resp


def main():
    app.run(host='0.0.0.0')


if __name__ == "__main__":
    main()
