from flask import Flask, request, render_template, jsonify, session
from flask_socketio import SocketIO, emit
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from collections import deque
import re
import threading
import uuid
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time

app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', str(uuid.uuid4()))  # Secure key
socketio = SocketIO(app, async_mode='threading', cors_allowed_origins="*")

# Thread-safe storage
results = {}
progress_logs = {}
results_lock = threading.Lock()

def load_config():
    """Load configuration from config.json or return defaults."""
    default_config = {
        "max_pages": 50,
        "max_depth": 7,
        "timeout": 10,
        "threads": 5,
        "retries": 3,
        "session_timeout": 3600,  # 1 hour
        "debug": True
    }
    try:
        if os.path.exists('config.json'):
            with open('config.json', 'r') as f:
                config = json.load(f)
            default_config.update(config)
    except Exception as e:
        print(f"Error loading config: {e}")
    return default_config

config = load_config()

def clean_email(email):
    """Validate and clean email addresses."""
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
        return None
    invalid_domains = {'example.com', 'test.com', 'invalid.com'}
    domain = email.split('@')[-1].lower()
    if domain in invalid_domains:
        return None
    return email

def crawl_page(url, session_obj, visited, max_depth, current_depth, emails, session_id, lock):
    """Crawl a single page and extract emails."""
    if current_depth > max_depth or url in visited:
        return []

    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
        resp = session_obj.get(url, timeout=config['timeout'], headers=headers)
        if resp.status_code != 200 or 'text/html' not in resp.headers.get('Content-Type', ''):
            socketio.emit('progress', {'session_id': session_id,
                                      'message': f"Skipped {url}: Invalid response"}, namespace='/crawl')
            return []

        soup = BeautifulSoup(resp.text, 'html.parser')
        text = soup.get_text()
        found_emails = re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text)

        page_emails = set()
        for email in found_emails:
            cleaned = clean_email(email)
            if cleaned:
                page_emails.add(cleaned)

        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.startswith('mailto:'):
                email = href[7:].split('?')[0].strip()
                cleaned = clean_email(email)
                if cleaned:
                    page_emails.add(cleaned)

        with lock:
            emails.update(page_emails)
            visited.add(url)
            progress_logs[session_id].append(f"Crawled {url}: Found {len(page_emails)} emails")
            socketio.emit('progress', {'session_id': session_id,
                                      'message': f"Crawled {url}: Found {len(page_emails)} emails"},
                         namespace='/crawl')

        links = []
        if current_depth < max_depth:
            for link in soup.find_all('a', href=True):
                new_url = urljoin(url, link['href'])
                parsed_new = urlparse(new_url)
                if parsed_new.scheme not in ('http', 'https'):
                    continue
                current_domain = urlparse(url).netloc
                if parsed_new.netloc != current_domain:
                    continue
                if new_url not in visited:
                    if any(keyword in new_url.lower() for keyword in ['contact', 'about', 'staff', 'faculty']):
                        links.insert(0, (new_url, current_depth + 1))
                    else:
                        links.append((new_url, current_depth + 1))
        return links

    except Exception as e:
        if config['debug']:
            print(f"Error crawling {url}: {e}")
        socketio.emit('progress', {'session_id': session_id,
                                  'message': f"Error crawling {url}: {str(e)}"}, namespace='/crawl')
        return []

def crawl_website(start_url, session_id):
    """Crawl a website starting from the given URL and extract emails."""
    emails = set()
    visited = set()
    to_visit = deque([(start_url, 0)])
    pages_crawled = 0

    session_obj = requests.Session()
    retries = Retry(total=config['retries'], backoff_factor=1,
                    status_forcelist=[500, 502, 503, 504])
    session_obj.mount('http://', HTTPAdapter(max_retries=retries))
    session_obj.mount('https://', HTTPAdapter(max_retries=retries))

    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=config['threads']) as executor:
        while to_visit and pages_crawled < config['max_pages']:
            futures = []
            batch_size = min(config['threads'], len(to_visit))
            for _ in range(batch_size):
                if to_visit:
                    url, depth = to_visit.popleft()
                    futures.append(executor.submit(crawl_page, url, session_obj,
                                                  visited, config['max_depth'],
                                                  depth, emails, session_id, lock))

            for future in as_completed(futures):
                new_links = future.result() or []
                for link in new_links:
                    if link[0] not in visited:
                        to_visit.append(link)
                pages_crawled += 1
                if config['debug']:
                    print(f"Crawled {pages_crawled}/{config['max_pages']} pages, found {len(emails)} emails")
                with lock:
                    socketio.emit('progress', {'session_id': session_id,
                                              'message': f"Progress: {pages_crawled}/{config['max_pages']} pages crawled, {len(emails)} emails found"},
                                 namespace='/crawl')

    with results_lock:
        results[session_id] = list(emails)
        socketio.emit('progress', {'session_id': session_id,
                                  'message': f"Crawling completed: Found {len(emails)} emails"},
                     namespace='/crawl')

@app.route('/')
def index():
    """Render the main page with the URL input form."""
    session_id = session.get('session_id')
    if not session_id:
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        session['timestamp'] = time.time()
        with results_lock:
            results[session_id] = []
            progress_logs[session_id] = []

    # Clean up old sessions
    with results_lock:
        current_time = time.time()
        expired = []
        for sid in list(progress_logs.keys()):
            ts = session.get('timestamp', 0) if session.get('session_id') == sid else 0
            if current_time - ts > config['session_timeout']:
                expired.append(sid)
        for sid in expired:
            results.pop(sid, None)
            progress_logs.pop(sid, None)

    return render_template('index.html',
                          emails=results.get(session_id, []),
                          logs=progress_logs.get(session_id, []),
                          session_id=session_id)

@app.route('/scrape', methods=['POST'])
def scrape():
    """Handle the URL submission and start crawling in a background thread."""
    url = request.form.get('url')
    if not url:
        return jsonify({'error': 'URL is required'}), 400

    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url

    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return jsonify({'error': 'Invalid URL'}), 400
    except Exception:
        return jsonify({'error': 'Invalid URL'}), 400

    session_id = session.get('session_id')
    if not session_id:
        session_id = str(uuid.uuid4())
        session['session_id'] = session_id
        session['timestamp'] = time.time()
        with results_lock:
            results[session_id] = []
            progress_logs[session_id] = []

    socketio.emit('progress', {'session_id': session_id,
                              'message': f"Initializing scrape for {url}"}, namespace='/crawl')

    def background_crawl():
        crawl_website(url, session_id)

    threading.Thread(target=background_crawl, daemon=True).start()
    return jsonify({'status': 'Scrape initiated', 'session_id': session_id})

@app.route('/results/<session_id>')
def get_results(session_id):
    """Return the current list of emails and logs for the session."""
    with results_lock:
        emails = results.get(session_id, [])
        logs = progress_logs.get(session_id, [])
    return jsonify({'emails': emails, 'logs': logs})

@socketio.on('connect', namespace='/crawl')
def handle_connect():
    """Handle WebSocket connection."""
    if config['debug']:
        print("WebSocket connected")

if __name__ == '__main__':
    os.makedirs('templates', exist_ok=True)
    socketio.run(app, debug=config['debug'], host='0.0.0.0', port=5000)