from partition_monitor import PartitionMonitor
from flask import Flask
from flask import render_template
from flask import request
from flask import jsonify
from flask_cors import CORS
import logging
from apscheduler.schedulers.background import BackgroundScheduler

import time 
application = Flask(__name__)
cors = CORS(application, resources={r"/*": {"origins": "*"}})
logging.basicConfig()

@application.route('/')
def index():

  Monitor = PartitionMonitor()
  return jsonify(Monitor.get_data())

@application.route('/history')
def history():

  cols = list()
  args = dict()
  limit = False
  offset = False

  if len(request.args):

      args = request.args.to_dict()

      if 'cols' in args:
        args.pop('cols')

  if request.args.get('cols'):

    cols = request.args.get('cols')

  if request.args.get('limit'):
    limit = int(request.args.get('limit'))
    args.pop('limit')

  if request.args.get('offset'):
    offset = int(request.args.get('offset'))
    args.pop('offset')

  Monitor = PartitionMonitor()
  return jsonify(Monitor.get_history(cols,args,limit,offset))

@application.route('/server_history')
def server_history():

  cols = list()
  args = dict()
  limit = False
  offset = False

  if len(request.args):

      args = request.args.to_dict()

      if 'cols' in args:
        args.pop('cols')

  if request.args.get('cols'):

    cols = request.args.get('cols')

  if request.args.get('limit'):
    limit = int(request.args.get('limit'))
    args.pop('limit')

  if request.args.get('offset'):
    offset = int(request.args.get('offset'))
    args.pop('offset')

  Monitor = PartitionMonitor()
  return jsonify(Monitor.get_server_history(cols,args,limit,offset))

@application.route('/update_db')

def update_db():
  
  with application.app_context():

    Monitor = PartitionMonitor()
    Monitor.update_db()

@application.route('/get_response')
def get_response():
  Monitor = PartitionMonitor()
  return jsonify(Monitor.get_data())

if __name__ == '__main__':

  scheduler = BackgroundScheduler()
  scheduler.add_job(update_db, 'interval', minutes=5,max_instances=1)
  scheduler.start()

  application.run(host='186.232.60.33', port=5005, debug=True)
