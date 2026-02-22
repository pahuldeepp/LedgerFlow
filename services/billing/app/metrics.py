# metrics.py - Expose Prometheus metrics
from prometheus_client import start_http_server, Counter

# Define Prometheus metrics
successful_events = Counter('successful_events', 'Number of successful events processed')
failed_events = Counter('failed_events', 'Number of failed events')

# Start Prometheus metrics server
start_http_server(8000)