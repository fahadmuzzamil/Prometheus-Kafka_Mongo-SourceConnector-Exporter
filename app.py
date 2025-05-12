import os
from flask import Flask, Response
from prometheus_client import generate_latest, Gauge
import requests
import time
import threading

app = Flask(__name__)

# Prometheus metrics
connection_state = Gauge('connection_state', 'Health of connection to Kafka Connect', ['connection'])
connector_state = Gauge('connector_state', 'State of Kafka Connect connectors', ['connector_name'])

# Get URL from environment variable (default to localhost)
KAFKA_CONNECT_URL = os.getenv('KAFKA_CONNECT_URL', 'http://localhost:8083')

def get_connector_list():
    try:
        response = requests.get(f"{KAFKA_CONNECT_URL}/connectors")
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching connector list: {e}")
        connection_state.labels(connection='connectors_list').set(0)
        return []

def scrape_connector_status(connector_name):
    try:
        url = f"{KAFKA_CONNECT_URL}/connectors/{connector_name}/status"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        state = data['tasks'][0]['state'] if data.get('tasks') else 'FAILED'
        connector_state.labels(connector_name=connector_name).set(1 if state == 'RUNNING' else 0)

        connection_state.labels(connection='working').set(1)
    except Exception as e:
        print(f"Error checking connector '{connector_name}': {e}")
        connector_state.labels(connector_name=connector_name).set(0)
        connection_state.labels(connection='working').set(0)

def monitor_connectors():
    while True:
        connectors = get_connector_list()
        for connector in connectors:
            scrape_connector_status(connector)
        time.sleep(10)

@app.route('/metrics')
def metrics():
    return Response(generate_latest(), mimetype='text/plain')

if __name__ == '__main__':
    threading.Thread(target=monitor_connectors).start()
    app.run(host='0.0.0.0', port=8001, debug=False, threaded=True)
