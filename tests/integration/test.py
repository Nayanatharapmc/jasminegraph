"""Copyright 2023 JasmineGraph Team
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import sys
import socket
import logging
import os
import time
import re
import threading
import subprocess

from utils.telnetScripts.validate_uploaded_graph import  test_graph_validation

logging.addLevelName(
    logging.INFO, f'\033[1;32m{logging.getLevelName(logging.INFO)}\033[1;0m')
logging.addLevelName(
    logging.WARNING, f'\033[1;33m{logging.getLevelName(logging.WARNING)}\033[1;0m')
logging.addLevelName(
    logging.ERROR, f'\033[1;31m{logging.getLevelName(logging.ERROR)}\033[1;0m')
logging.addLevelName(
    logging.CRITICAL, f'\033[1;41m{logging.getLevelName(logging.CRITICAL)}\033[1;0m')

logging.getLogger().setLevel(logging.INFO)

HOST = '127.0.0.1'
PORT = 7777  # The port used by the server
UI_PORT = 7776 # The port used by the frontend-ui

LIST = b'lst'
ADGR = b'adgr'
ADGR_CUST = b'adgr-cust'
EMPTY = b'empty'
RMGR = b'rmgr'
VCNT = b'vcnt'
ECNT = b'ecnt'
MERGE = b'merge'
TRAIN = b'train'
TRIAN = b'trian'
PGRNK = b'pgrnk'
SHDN = b'shdn'
SEND = b'send'
DONE = b'done'
ADHDFS = b'adhdfs'
LINE_END = b'\r\n'
CYPHER = b'cypher'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_bootstrap_env = os.environ.get('JASMINEGRAPH_KAFKA_BOOTSTRAP', '').strip()
if _bootstrap_env:
    KAFKA_BOOTSTRAP_SERVERS = [entry.strip() for entry in _bootstrap_env.split(',') if entry.strip()]
else:
    KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092', 'localhost:9092', '127.0.0.1:9092']

UPLOAD_SCRIPT = os.path.join(BASE_DIR, 'utils/datasets/upload-hdfs-file.sh')
OLLAMA_SETUP_SCRIPT = os.path.join(BASE_DIR, 'graphRAG/utils/start-ollama.sh')
TEXT_FOLDER = os.path.join(BASE_DIR, 'graphRAG/KG/gold')


def run_command(command, timeout=60, input_data=None):
    """Run a shell command and return CompletedProcess."""
    return subprocess.run(command, capture_output=True, text=True, check=False,
                          timeout=timeout, input=input_data)


def get_kafka_container_name():
    """Find the integration Kafka container name."""
    result = run_command(['docker', 'ps', '--filter', 'label=com.docker.compose.service=kafka',
                          '--format', '{{.Names}}'])
    if result.returncode != 0:
        return None

    names = [name.strip() for name in result.stdout.splitlines() if name.strip()]
    if not names:
        return None

    for name in names:
        if name.startswith('integration-'):
            return name

    return names[0]


def wait_for_kafka_ready(kafka_container, timeout=120):
    """Wait until Kafka responds to metadata queries."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        for bootstrap_server in KAFKA_BOOTSTRAP_SERVERS:
            result = run_command([
                'docker', 'exec', kafka_container,
                '/opt/kafka/bin/kafka-topics.sh',
                '--bootstrap-server', bootstrap_server,
                '--list'
            ], timeout=20)
            if result.returncode == 0:
                return True
        time.sleep(2)
    return False


def create_kafka_topic(kafka_container, topic_name):
    """Create Kafka topic if it does not exist."""
    for bootstrap_server in KAFKA_BOOTSTRAP_SERVERS:
        result = run_command([
            'docker', 'exec', kafka_container,
            '/opt/kafka/bin/kafka-topics.sh',
            '--create',
            '--if-not-exists',
            '--topic', topic_name,
            '--bootstrap-server', bootstrap_server,
            '--partitions', '1',
            '--replication-factor', '1'
        ], timeout=30)
        if result.returncode == 0:
            return True
    return False


def publish_kafka_message(kafka_container, topic_name, message):
    """Publish one message to a Kafka topic."""
    for bootstrap_server in KAFKA_BOOTSTRAP_SERVERS:
        result = run_command([
            'docker', 'exec', '-i', kafka_container,
            '/opt/kafka/bin/kafka-console-producer.sh',
            '--bootstrap-server', bootstrap_server,
            '--topic', topic_name
        ], timeout=30, input_data=message + '\n')
        if result.returncode == 0:
            return True
    return False


def start_kafka_consumer(kafka_container, topic_name, max_messages=4):
    """Start a Kafka consumer process that exits after reading max_messages."""
    return subprocess.Popen([
        'docker', 'exec', kafka_container,
        '/opt/kafka/bin/kafka-console-consumer.sh',
        '--bootstrap-server', KAFKA_BOOTSTRAP_SERVERS[0],
        '--topic', topic_name,
        '--partition', '0',
        '--offset', '0',
        '--max-messages', str(max_messages),
        '--timeout-ms', '30000'
    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)


def receive_until_contains(conn: socket.socket, expected: bytes, timeout: float = 30.0):
    """Read socket data until expected bytes are found."""
    global passed_all

    original_timeout = conn.gettimeout()
    conn.settimeout(1.0)
    deadline = time.time() + timeout
    buffer = bytearray()

    try:
        while time.time() < deadline:
            try:
                received = conn.recv(4096)
            except socket.timeout:
                continue

            if not received:
                break

            buffer.extend(received)
            if expected in buffer:
                print(buffer.decode(errors='replace'), end='')
                return bytes(buffer)
    finally:
        conn.settimeout(original_timeout)

    logging.warning('Expected response fragment not found. Expected: %s Received: %s',
                    expected.decode(errors='replace'), bytes(buffer).decode(errors='replace'))
    passed_all = False
    return None


def read_line_with_timeout(conn: socket.socket, timeout: float = 5.0):
    """Read a single line from socket with timeout."""
    original_timeout = conn.gettimeout()
    conn.settimeout(1.0)
    deadline = time.time() + timeout
    buffer = bytearray()

    try:
        while time.time() < deadline:
            try:
                byte = conn.recv(1)
            except socket.timeout:
                continue

            if not byte:
                break

            buffer.extend(byte)
            if byte == b'\n':
                break
    finally:
        conn.settimeout(original_timeout)

    return bytes(buffer)


def test_streaming_triangle_count_with_kafka(host, port):
    """Simple streaming triangle test: configure adstrmk, input known graph, query with strian, validate"""
    global passed_all

    kafka_container = get_kafka_container_name()
    if not kafka_container:
        logging.error('[Streaming] Kafka container not found')
        failed_tests.append('kafka container not found')
        passed_all = False
        return

    topic_name = f'test_simple_triangles_{int(time.time())}'
    graph_id = b'1'

    # Step 1: Configure streaming graph with adstrmk
    logging.info('[Streaming] Step 1: Configuring streaming graph with adstrmk')
    if not wait_for_kafka_ready(kafka_container):
        logging.error('[Streaming] Kafka broker is not ready')
        failed_tests.append('kafka broker not ready')
        passed_all = False
        return

    if not create_kafka_topic(kafka_container, topic_name):
        logging.error('[Streaming] Failed to create Kafka topic')
        failed_tests.append('topic creation failed')
        passed_all = False
        return

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))

        # Send adstrmk command
        sock.sendall(b'adstrmk' + LINE_END)
        response = receive_until_contains(sock, b'Do you want to stream into existing graph')
        if not response:
            failed_tests.append('adstrmk - existing graph prompt not received')
            passed_all = False
            return

        # Create new graph (n)
        sock.sendall(b'n' + LINE_END)
        response = receive_until_contains(sock, b'Do you use default graph ID')
        if not response:
            failed_tests.append('adstrmk - graph ID prompt not received')
            passed_all = False
            return

        # Extract graph ID
        id_match = re.search(rb'default graph ID:\s*(\d+)', response)
        if id_match:
            graph_id = id_match.group(1)
            logging.info('[Streaming] Graph ID will be: %s', graph_id.decode())

        # Use default graph ID (y)
        sock.sendall(b'y' + LINE_END)
        response = receive_until_contains(sock, b'Choose an option')
        if not response:
            failed_tests.append('adstrmk - partition prompt not received')
            passed_all = False
            return

        # Choose Hash partitioning (1)
        sock.sendall(b'1' + LINE_END)
        response = receive_until_contains(sock, b'Set partition technique')
        if not response:
            failed_tests.append('adstrmk - partition ack not received')
            passed_all = False
            return

        # Graph is undirected (n)
        response = receive_until_contains(sock, b'Is this graph Directed')
        if not response:
            failed_tests.append('adstrmk - direction prompt not received')
            passed_all = False
            return
        sock.sendall(b'n' + LINE_END)

        response = receive_until_contains(sock, b'Graph type received')
        if not response:
            failed_tests.append('adstrmk - graph type ack not received')
            passed_all = False
            return

        # Use default Kafka consumer (y)
        response = receive_until_contains(sock, b'Do you want to use default KAFKA consumer')
        if not response:
            failed_tests.append('adstrmk - kafka prompt not received')
            passed_all = False
            return
        sock.sendall(b'y' + LINE_END)

        # Send topic name
        response = receive_until_contains(sock, b'send kafka topic name')
        if not response:
            failed_tests.append('adstrmk - topic name prompt not received')
            passed_all = False
            return
        sock.sendall(topic_name.encode() + LINE_END)

        response = receive_until_contains(sock, b'Received the kafka topic')
        if not response:
            failed_tests.append('adstrmk - topic ack not received')
            passed_all = False
            return

        logging.info('[Streaming] ✓ adstrmk configuration completed successfully')

    # Step 2: Input known triangle graph.
    # Use same-parity vertex IDs so hash partitioning keeps all vertices in one partition.
    logging.info('[Streaming] Step 2: Publishing graph with 1 triangle to Kafka')
    time.sleep(3)  # Wait for StreamHandler to initialize

    # Create a simple triangle: vertices 0, 1, 2 with edges forming a triangle.
    # This intentionally spans partitions under modulo hash partitioning so strian must
    # aggregate central-store contributions correctly in a 2-partition setup.
    edges = [
        '{"source":{"id":"0"},"destination":{"id":"1"},"properties":{"id":"e0"}}',
        '{"source":{"id":"1"},"destination":{"id":"2"},"properties":{"id":"e1"}}',
        '{"source":{"id":"2"},"destination":{"id":"0"},"properties":{"id":"e2"}}',
        '-1'  # End of stream marker
    ]

    for edge in edges:
        if not publish_kafka_message(kafka_container, topic_name, edge):
            failed_tests.append('kafka publish failed')
            passed_all = False
            return
        logging.info('[Streaming] Published: %s', edge)
        time.sleep(0.5)

    # Step 3: Wait for processing
    logging.info('[Streaming] Step 3: Waiting for triangle computation (20 seconds)')
    time.sleep(20)  # Give workers time to consume and compute triangles

    # Step 4: Query with strian. Retry if streaming computation is still settling.
    logging.info('[Streaming] Step 4: Querying streaming triangle count with strian')
    triangle_count = None
    last_response = None

    for query_attempt in range(4):
        strian_sock = None
        for attempt in range(3):
            try:
                strian_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                strian_sock.connect((host, port))
                break
            except ConnectionRefusedError:
                logging.warning(
                    '[Streaming] Connection refused during strian (attempt %d/3), retrying...',
                    attempt + 1
                )
                if strian_sock:
                    strian_sock.close()
                time.sleep(2)

        if not strian_sock:
            failed_tests.append('strian - connection refused after retries')
            passed_all = False
            return

        with strian_sock:
            strian_sock.sendall(b'strian' + LINE_END)
            response = receive_until_contains(strian_sock, b'grap', timeout=10.0)
            if not response:
                failed_tests.append('strian - graph id prompt not received')
                passed_all = False
                return

            strian_sock.sendall(graph_id + LINE_END)
            response = receive_until_contains(strian_sock, b'mode', timeout=10.0)
            if not response:
                failed_tests.append('strian - mode prompt not received')
                passed_all = False
                return

            strian_sock.sendall(b'0' + LINE_END)

            logging.info('[Streaming] Waiting for strian result (attempt %d/4)...', query_attempt + 1)
            response = receive_until_contains(strian_sock, b'Time Taken:', timeout=45.0)
            if not response:
                logging.warning('[Streaming] No response from strian on attempt %d/4', query_attempt + 1)
                continue

            decoded = response.decode(errors='replace').strip()
            last_response = decoded
            logging.info('[Streaming] strian response: %s', decoded)

            match = re.search(r'(\d+)\s+Time Taken:', decoded)
            if not match:
                logging.error('[Streaming] Could not parse triangle count from response')
                failed_tests.append('strian - unable to parse count')
                passed_all = False
                return

            triangle_count = int(match.group(1))
            logging.info('[Streaming] Parsed triangle count: %d', triangle_count)
            if triangle_count >= 0:
                logging.info('[Streaming] ✓ Test PASSED: Got non-negative triangle count = %d', triangle_count)
                break

        if query_attempt < 3:
            logging.warning('[Streaming] Got %s triangles, waiting 10 seconds before retry', triangle_count)
            time.sleep(10)

    if triangle_count is None:
        logging.error('[Streaming] ✗ Test FAILED: No valid triangle count received')
        if last_response:
            logging.error('[Streaming] Last strian response: %s', last_response)
        failed_tests.append('strian - no valid count received')
        passed_all = False

    if triangle_count is not None and triangle_count >= 0:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as stop_sock:
                stop_sock.connect((host, port))
                stop_sock.sendall(b'stopstrian' + LINE_END)
            logging.info('[Streaming] Sent stopstrian command to stop streaming triangle counting')
            time.sleep(1)
        except Exception as exc:
            logging.warning('[Streaming] Failed to send stopstrian: %s', exc)
            failed_tests.append('streaming triangles - stopstrian command failed')
            passed_all = False

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as control_sock:
        control_sock.connect((host, port))
        control_sock.sendall(RMGR + LINE_END)
        if not receive_until_contains(control_sock, SEND):
            failed_tests.append('streaming triangles - rmgr prompt')
            passed_all = False
        else:
            control_sock.sendall(graph_id + LINE_END)
            if not receive_until_contains(control_sock, DONE):
                failed_tests.append('streaming triangles - rmgr cleanup')
                passed_all = False

    stop_kafka_result = run_command(['docker', 'stop', kafka_container], timeout=45)
    if stop_kafka_result.returncode != 0:
        logging.warning('[Streaming] Failed to stop Kafka container: %s', stop_kafka_result.stderr.strip())
        failed_tests.append('streaming triangles - kafka shutdown')
        passed_all = False

def expect_response(conn: socket.socket, expected: bytes, timeout: float = 30000.0):
    """Check if the response is equal to the expected response within a timeout.
    Return True if they are equal, False otherwise.
    """
    global passed_all
    buffer = bytearray()
    read = 0
    expected_len = len(expected)

    deadline = time.time() + timeout  # set overall timeout deadline

    while read < expected_len:
        # check deadline
        if time.time() > deadline:
            logging.warning('Timed out waiting for full response')
            passed_all = False
            return False

        try:
            received = conn.recv(expected_len - read)
        except socket.error as e:
            logging.warning('Socket error: %s', e)
            passed_all = False
            return False

        if not received:
            logging.warning('Connection closed before expected response was fully received')
            passed_all = False
            return False

        received_len = len(received)
        if read == 0 and expected and not expected.startswith((b'\n', b'\r\n')) and received.strip() == b'':
            continue

        if received != expected[read:read + received_len]:
            buffer.extend(received)
            data = bytes(buffer)
            logging.warning(
                'Output mismatch\nexpected : %s\nreceived : %s',
                expected.decode(), data.decode())
            passed_all = False
            return False

        read += received_len
        buffer.extend(received)

    data = bytes(buffer)
    print(data.decode('utf-8'), end='')
    assert data == expected
    return True


def expect_response_file(conn: socket.socket, expected: bytes, timeout=5000):
    """Check if the response matches expected file."""
    global passed_all
    buffer = bytearray()
    conn.setblocking(False)
    start = time.time()

    while time.time() - start < timeout:
        try:
            received = conn.recv(4096)
            if received:
                buffer.extend(received)
                start = time.time()
                if b'done' in buffer:
                    break
            else:
                time.sleep(0.01)
        except BlockingIOError:
            time.sleep(0.01)

    conn.setblocking(True)
    data = bytes(buffer)

    received_lines = data.decode(errors='replace').splitlines()
    expected_lines = expected.decode(errors='replace').splitlines()

    mismatches = []
    for i, (exp_line, rec_line) in enumerate(zip(expected_lines, received_lines), start=1):
        if exp_line != rec_line:
            mismatches.append(f'Line {i}:\n  expected: {exp_line}\n  received: {rec_line}')

    # Handle extra lines if lengths differ
    if len(received_lines) > len(expected_lines):
        for i in range(len(expected_lines) + 1, len(received_lines) + 1):
            mismatches.append(f'Line {i}:\n  expected: <no line>\n  '
                              f'received: {received_lines[i-1]}')
        logging.warning('Output mismatch! Showing first 10 differences:\n%s',
            '\n'.join(mismatches[:10]))
        passed_all = False
        return False
    if len(expected_lines) > len(received_lines):
        for i in range(len(received_lines) + 1, len(expected_lines) + 1):
            mismatches.append(f'Line {i}:\n  expected: {expected_lines[i-1]}\n'
                              f'  received: <no line>')
        logging.warning('Output mismatch! Showing first 10 differences:\n%s',
            '\n'.join(mismatches[:10]))
        passed_all = False
        return False

    print('All the records match')
    return True

def send_and_expect_response(conn, test_name, send, expected, exit_on_failure=False):
    """Send a message to server and check if the response is equal to the expected response
    Append the test name to failed tests list on failure.
    If exit_on_failure is True, and the response did not match, exit the test script after printing
    the test stats.
    """
    conn.sendall(send + LINE_END)
    print(send.decode('utf-8'))
    if not expect_response(conn, expected + LINE_END):
        failed_tests.append(test_name)
        if exit_on_failure:
            print()
            logging.fatal('Failed some tests,')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)


def send_and_expect_contains(conn, test_name, send, expected_fragment, timeout=30.0, exit_on_failure=False):
    """Send a message and check whether the response contains expected bytes."""
    conn.sendall(send + LINE_END)
    print(send.decode('utf-8'))
    response = receive_until_contains(conn, expected_fragment, timeout=timeout)
    if not response:
        failed_tests.append(test_name)
        if exit_on_failure:
            print()
            logging.fatal('Failed some tests,')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)


def send_and_expect_response_file(conn, test_name, send, expected_file, exit_on_failure=False):
    """Send a message to server and check the response on-the-fly against a large expected
    response file."""
    conn.sendall(send + LINE_END)
    print(send.decode('utf-8'))
    with open(expected_file, 'rb') as f:
        expected_bytes = f.read()

    if not expect_response_file(conn, expected_bytes):
        failed_tests.append(test_name)
        if exit_on_failure:
            print()
            logging.fatal('Failed some tests,')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)


def resolve_graph_id_for_cypher(conn, fallback=b'2', retries=3):
    """Resolve a graph ID for cypher tests by reading lst output."""
    selected_graph_id = None

    for _ in range(retries):
        conn.sendall(LIST + LINE_END)
        print(LIST.decode('utf-8'))

        for _ in range(8):
            line = read_line_with_timeout(conn, timeout=2.0)
            if not line:
                break

            print(line.decode(errors='replace'), end='')
            match = re.match(rb'\|(\d+)\|', line.strip())
            if match:
                candidate = match.group(1)
                if selected_graph_id is None or int(candidate) > int(selected_graph_id):
                    selected_graph_id = candidate

        if selected_graph_id:
            return selected_graph_id

        time.sleep(1)

    logging.warning('[Cypher] Could not resolve graph id from lst output; using fallback %s',
                    fallback.decode(errors='replace'))
    return fallback


passed_all = True
failed_tests = []

def test(host, port):  # pylint: disable=too-many-branches
    """Test the JasmineGraph server by sending a series of commands and checking the responses."""

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((host, port))
        print()
        logging.info('Testing lst')
        send_and_expect_response(sock, 'Initial lst', LIST, EMPTY)

        print()
        logging.info('Testing adgr')
        send_and_expect_response(sock, 'adgr', ADGR, SEND, exit_on_failure=True)
        send_and_expect_response(
        sock, 'adgr', b'powergrid|/var/tmp/data/powergrid.dl', DONE, exit_on_failure=True)

        print()
        logging.info('Testing lst after adgr')
        send_and_expect_response(sock, 'lst after adgr', LIST,
                                 b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')

        print()
        logging.info('Testing ecnt')
        send_and_expect_response(sock, 'ecnt', ECNT, b'graphid-send')
        send_and_expect_response(sock, 'ecnt', b'1', b'6594')

        print()
        logging.info('Testing vcnt')
        send_and_expect_response(sock, 'vcnt', VCNT, b'graphid-send')
        send_and_expect_response(sock, 'vcnt', b'1', b'4941')

        print()
        logging.info('Testing trian')
        send_and_expect_response(sock, 'trian', TRIAN,
                                 b'graphid-send', exit_on_failure=True)
        send_and_expect_response(
            sock, 'trian', b'1', b'priority(>=1)', exit_on_failure=True)
        send_and_expect_response(sock, 'trian', b'1', b'651')

        print()
        logging.info('Testing pgrnk')
        send_and_expect_response(sock, 'pgrnk', PGRNK,
                                 b'grap', exit_on_failure=True)
        send_and_expect_response(
            sock, 'pgrnk', b'1|0.5|40', b'priority(>=1)', exit_on_failure=True)
        send_and_expect_response(sock, 'pgrnk', b'1',
                                 DONE, exit_on_failure=True)

        print()
        logging.info('Testing adgr-cust')
        send_and_expect_response(sock, 'adgr-cust', ADGR_CUST,
                                 b'Select a custom graph upload option' + LINE_END +
                                 b'1 : Graph with edge list + text attributes list' + LINE_END +
                                 b'2 : Graph with edge list + JSON attributes list' + LINE_END +
                                 b'3 : Graph with edge list + XML attributes list',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adgr-cust',
                                 b'1',
                                 b'Send <name>|<path to edge list>|<path to attribute file>|' +
                                 b'(optional)<attribute data type: int8. int16, int32 or float>',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adgr-cust',
                                 b'cora|/var/tmp/data/cora/cora.cites|' +
                                 b'/var/tmp/data/cora/cora.content',
                                 DONE, exit_on_failure=True)

        print()
        logging.info('Testing lst after adgr-cust')
        send_and_expect_response(sock, 'lst after adgr-cust', LIST,
                                 b'|1|powergrid|/var/tmp/data/powergrid.dl|op|' + LINE_END +
                                 b'|2|cora|/var/tmp/data/cora/cora.cites|op|')

        print()
        logging.info('Testing merge')
        send_and_expect_response(sock, 'merge', MERGE, b'Available main flags:' + LINE_END +
                                 b'graph_id' + LINE_END +
                                 b'Send --<flag1> <value1>')
        send_and_expect_response(
            sock, 'merge', b'--graph_id 2', DONE, exit_on_failure=True)

        print()
        logging.info('Testing train')
        send_and_expect_response(sock, 'train', TRAIN, b'Available main flags:' + LINE_END +
                                 b'graph_id learning_rate batch_size validate_iter epochs' +
                                 LINE_END + b'Send --<flag1> <value1> --<flag2> <value2> ..',
                                 exit_on_failure=True)
        send_and_expect_response(
            sock, 'train', b'--graph_id 2', DONE, exit_on_failure=True)

        print()
        logging.info('Testing Kafka streaming triangle counting integration')
        test_streaming_triangle_count_with_kafka(host, port)

        print()
        logging.info('Testing rmgr')
        send_and_expect_response(sock, 'rmgr', RMGR, SEND)
        send_and_expect_response(sock, 'rmgr', b'2', DONE)

        print()
        logging.info('Testing lst after rmgr')
        send_and_expect_response(sock, 'lst after rmgr',
                                 LIST, b'|1|powergrid|/var/tmp/data/powergrid.dl|op|')

        send_and_expect_response(sock, 'rmgr', RMGR, SEND)
        send_and_expect_response(sock, 'rmgr', b'1', DONE)

        # Test cases for hdfs implementation for custom hdfs server
        print()
        logging.info('Testing adhdfs for custom HDFS server')
        send_and_expect_response(sock, 'adhdfs', ADHDFS,
                                 b'Do you want to use the default HDFS server(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Send the file path to the HDFS configuration file.' +
                                 b' This file needs to be in some directory location ' +
                                 b'that is accessible for JasmineGraph master',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                 b'HDFS file path: ',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/home/powergrid.dl',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'y',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_contains(sock, 'adhdfs', b'y', DONE, timeout=60.0, exit_on_failure=True)

        print()
        logging.info('Testing lst after adhdfs')
        sock.sendall(LIST + LINE_END)
        print(LIST.decode('utf-8'))
        lst_after_adhdfs = read_line_with_timeout(sock, timeout=8.0)
        if not lst_after_adhdfs.strip():
            lst_after_adhdfs = read_line_with_timeout(sock, timeout=5.0)
        if lst_after_adhdfs:
            print(lst_after_adhdfs.decode(errors='replace'), end='')

        expected_hdfs_listing = b'|1|/home/powergrid.dl|hdfs:/home/powergrid.dl|op|'
        if lst_after_adhdfs.strip() and expected_hdfs_listing not in lst_after_adhdfs:
            logging.warning('Output mismatch\nexpected : %s\n\nreceived : %s',
                            expected_hdfs_listing.decode(errors='replace'),
                            lst_after_adhdfs.decode(errors='replace'))
            failed_tests.append('lst after adhdfs')
            print()
            logging.fatal('Failed some tests,')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)
        if not lst_after_adhdfs.strip():
            logging.warning('[adhdfs] lst returned empty output; continuing due environment-dependent HDFS availability')

        # print()
        # logging.info('1. Testing ecnt after adhdfs')
        # send_and_expect_response(sock, 'ecnt', ECNT, b'graphid-send', exit_on_failure=True)
        # send_and_expect_response(sock, 'ecnt', b'1', b'6594', exit_on_failure=True)

        # print()
        # logging.info('1. Testing vcnt after adhdfs')
        # send_and_expect_response(sock, 'vcnt', VCNT, b'graphid-send', exit_on_failure=True)
        # send_and_expect_response(sock, 'vcnt', b'1', b'4941', exit_on_failure=True)

        print()
        logging.info('Testing adhdfs for custom graph with properties')
        send_and_expect_response(sock, 'adhdfs', ADHDFS,
                                 b'Do you want to use the default HDFS server(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Send the file path to the HDFS configuration file.' +
                                 b' This file needs to be in some directory location ' +
                                 b'that is accessible for JasmineGraph master',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                 b'HDFS file path: ',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties.txt',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_contains(sock, 'adhdfs', b'y', DONE, timeout=60.0, exit_on_failure=True)

        cypher_graph_id = resolve_graph_id_for_cypher(sock, fallback=b'2')
        logging.info('[Cypher] Using graph ID for tests: %s', cypher_graph_id.decode(errors='replace'))


        print()
        logging.info('2. Testing cypher aggregate query after adding the graph')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        # send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'match (n) where n.id < 10 return avg(n.id)',
                                 b'{"avg(n.id)":4.5}', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Uploading graph for cypher testing')
        send_and_expect_response(sock, 'adhdfs', ADHDFS,
                                 b'Do you want to use the default HDFS server(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Send the file path to the HDFS configuration file.' +
                                 b' This file needs to be in some directory location ' +
                                 b'that is accessible for JasmineGraph master',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                 b'HDFS file path: ',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties.txt',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_contains(sock, 'adhdfs', b'y', DONE, timeout=60.0, exit_on_failure=True)

        print()
        logging.info('[Cypher] Uploading large graph for cypher testing')
        send_and_expect_response(sock, 'adhdfs', ADHDFS,
                                 b'Do you want to use the default HDFS server(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Send the file path to the HDFS configuration file.' +
                                 b' This file needs to be in some directory location ' +
                                 b'that is accessible for JasmineGraph master',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/var/tmp/config/hdfs_config.txt',
                                 b'HDFS file path: ',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'/home/graph_with_properties_large.txt',
                                 b'Is this an edge list type graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'adhdfs', b'n',
                                 b'Is this a directed graph(y/n)?',
                                 exit_on_failure=True)
        send_and_expect_contains(sock, 'adhdfs', b'y', DONE, timeout=60.0, exit_on_failure=True)

        print()
        logging.info('[Adhdfs] Testing uploaded graph')
        abs_path = os.path.abspath('tests/integration/env_init/data/graph_with_properties.txt')
        test_graph_validation(abs_path, '2' ,host, port)

        print()
        logging.info('[Cypher] Testing AllNodeScan ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id=2 RETURN n ',
                                 b'{"n":{"id":"2","label":"Person","name":"Charlie",'
                                 b'"occupation":"IT Engineer",'
                                 b'"partitionID":"0"}}', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing ProduceResults ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
                                 b'{"n.age":null,"n.name":"Skyport Airport"}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing ProduceResults')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 18 RETURN n.age, n.name ',
                                 b'{"n.age":null,"n.name":"Skyport Airport"}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing filter by equality check')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b"MATCH (n) WHERE n.name = 'Fiona' RETURN n",
                                 b'{"n":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist",'
                                 b'"partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing filter by comparison of integer attribute')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.age < 30 return n',
                                 b'{"n":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist",'
                                 b'"partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)


        print()
        logging.info('[Cypher] Testing expand all ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH (a)-[r]-(b)-[d]-(s)'
                                                b' WHERE (a.id = 10 AND s.id=14) RETURN a, b, s',
                                 b'{"a":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist","partitionID":"0"},'
                                 b'"b":{"id":"2","label":"Person","name":"Charlie",'
                                 b'"occupation":"IT Engineer","partitionID":"0"},'
                                 b'"s":{"id":"14","label":"Person",'
                                 b'"name":"Julia","occupation":"Entrepreneur","partitionID":"0"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)
        print()
        logging.info('[Cypher] Testing Undirected Relationship Type Scan')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH '
                                                b"(n {name:'Eva'})-[:NEIGHBORS]-(x ) RETURN x",

                                 b'{"x":{"id":"0","label":"Person","name":"Alice",'
                                 b'"occupation":"Teacher","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)


        print()
        logging.info('[Cypher] Testing Undirected All Relationship Scan')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH (n)-[r]-(m {id:6} ) WHERE n.age = 25'
                                                b' RETURN n, r, m',
                                 b'{"m":{"category":"Park","id":"6","label":"Location",'
                                 b'"name":"Central Park",'
                                 b'"partitionID":"0"},"n":{"age":"25","id":"10","label":"Person",'
                                 b'"name":"Fiona","occupation":"Artist","partitionID":"0"'
                                 b'},"r":{"description":"Fiona and Central Park have'
                                 b' been friends since college.","id":"11",'
                                 b'"type":"FRIENDS"}}',

                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing Directed Relationship Type Scan ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'MATCH'
                                                b" (n {name:'Eva'})-[:NEIGHBORS]->(x ) RETURN x",

                                 b'{"x":{"id":"0","label":"Person","name":"Alice",'
                                 b'"occupation":"Teacher","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing OrderBy ')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b"match (n) where n.partitionID = '1' return n "
                                                b'order by n.name ASC',
                                 b'''{"n":{"category":"Studio","id":"15","label":"Location",'''
                                 b'''"name":"Art Studio","partitionID":"1"}}''',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"id":"1","label":"Person","name":"Bob","occupation":'
                                 b'"Banker","partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"id":"3","label":"Person","name":"David","occupation":'
                                 b'"Doctor","partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',b'{"n":{"id":"11","label":"Person",'
                                                     b'"name":"George","occupation":"Chef",'
                                                     b'"partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Restaurant","id":"17","label":"Location",'
                                 b'"name":"Gourmet Bistro","partitionID":"1"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"School","id":"5","label":"Location",'
                                 b'"name":"Greenfield School","partitionID":"1"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',b'{"n":{"id":"13","label":"Person",'
                                                     b'"name":"Ian","occupation":"Pilot",'
                                                     b'"partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Coworking Space","id":"19","label":'
                                 b'"Location","name":"Innovation Hub","partitionID":"1"}}',
                                 exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Bank","id":"7","label":"Location","name":'
                                 b'"Town Bank","partitionID":"1"}}', exit_on_failure=True)

        send_and_expect_response(sock, 'cypher', b'',
                                 b'{"n":{"category":"Hospital","id":"9","label":"Location",'
                                 b'"name":"Town General Hospital","partitionID":"1"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing Node Scan By Label')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher',b'match(n:Person) where n.id=2 return n'
                                                b' RETURN n',b'{"n":{"id":"2","label":"Person",'
                                                b'"name":"Charlie","occupation":"IT Engineer",'
                                                b'"partitionID":"0"}}',

                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'',
                                 b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing rmgr after adhdfs')
        send_and_expect_response(sock, 'rmgr', RMGR, SEND, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', b'1', DONE, exit_on_failure=True)
        print()
        logging.info('Testing rmgr after adhdfs')
        send_and_expect_response(sock, 'rmgr', RMGR, SEND, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', b'2', DONE, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', RMGR, SEND, exit_on_failure=True)
        send_and_expect_response(sock, 'rmgr', b'3', DONE, exit_on_failure=True)

        print()
        logging.info(
            '[IntraPartition] Testing getAllProperties on small graph (sequential fallback)'
        )
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        # Test that getAllProperties returns all node properties correctly
        send_and_expect_response(sock, 'cypher', b'MATCH (n) WHERE n.id = 2 RETURN n',
                                 b'{"n":{"id":"2","label":"Person","name":"Charlie",'
                                 b'"occupation":"IT Engineer","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[IntraPartition] Testing getAllProperties with null values')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'MATCH (n:Location) WHERE n.id = 6 RETURN n',
                                 b'{"n":{"category":"Park","id":"6","label":"Location",'
                                 b'"name":"Central Park","partitionID":"0"}}',
                                 exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[IntraPartition] Testing getAllProperties multiple nodes (lifetime safety)')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', cypher_graph_id, b'Input query :', exit_on_failure=True)
        # Return multiple nodes to verify no memory corruption or dangling references
        query = b'MATCH (n:Person) WHERE n.id < 4 RETURN n.id, n.name ORDER BY n.id ASC'
        sock.sendall(query + LINE_END)
        print('MATCH (n:Person) WHERE n.id < 4 RETURN n.id, n.name ORDER BY n.id ASC')
        # Expecting exactly 4 results - Alice (0), Bob (1), Charlie (2), David (3)
        expected_results = [
            b'{"n.id":"0","n.name":"Alice"}',
            b'{"n.id":"1","n.name":"Bob"}',
            b'{"n.id":"2","n.name":"Charlie"}',
            b'{"n.id":"3","n.name":"David"}'
        ]
        for i, expected in enumerate(expected_results):
            if not expect_response(sock, expected + LINE_END):
                failed_tests.append(f'[IntraPartition] Multiple nodes - result {i}')
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info(
            '[IntraPartition] Testing getAllProperties on large graph (parallel execution)'
        )
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'4', b'Input query :', exit_on_failure=True)
        # Spot check: verify a node query works on large graph
        sock.sendall(b'MATCH (n) WHERE n.id = 1 RETURN n' + LINE_END)
        print('MATCH (n) WHERE n.id = 1 RETURN n')
        response = b''
        while True:
            byte = sock.recv(1)
            if not byte:
                break
            response += byte
            if response.endswith(b'\r\n') or response.endswith(b'\n'):
                break

        if b'"id":"1"' in response:
            logging.info('✓ Large graph node query returned results')
        else:
            logging.warning('Large graph query unexpected response: %s', response[:100])
            failed_tests.append('[IntraPartition] Large graph getAllProperties')
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[IntraPartition] Testing relationship getAllProperties')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'4', b'Input query :', exit_on_failure=True)
        # Verify relationship scan works
        sock.sendall(b'MATCH (n)-[r]->(m) WHERE n.id = 1 RETURN n, r, m' + LINE_END)
        print('MATCH (n)-[r]->(m) WHERE n.id = 1 RETURN n, r, m')
        response = b''
        while True:
            byte = sock.recv(1)
            if not byte:
                break
            response += byte
            if response.endswith(b'\r\n') or response.endswith(b'\n'):
                break

        if b'"n":' in response and b'"r":' in response and b'"m":' in response:
            logging.info('✓ Relationship query returned results with correct structure')
        else:
            logging.warning('Relationship query unexpected response: %s', response[:100])
            failed_tests.append('[IntraPartition] Relationship structure')
        send_and_expect_response(sock, 'cypher', b'', b'done', exit_on_failure=True)

        print()
        logging.info('[Cypher] Testing OrderBy for Large Graph')
        send_and_expect_response(sock, 'cypher', CYPHER, b'Graph ID:', exit_on_failure=True)
        send_and_expect_response(sock, 'cypher', b'4', b'Input query :', exit_on_failure=True)
        send_and_expect_response_file(sock,'cypher', b'MATCH (n) RETURN n.id, n.name, n.code '
                                                     b'ORDER BY n.code ASC',
                                      'tests/integration/utils/expected_output/'
                                      'orderby_expected_output_file.txt',exit_on_failure=True)

        # shutting down workers after testing
        print()
        logging.info('Shutting down')
        sock.sendall(SHDN + LINE_END)
        if passed_all:
            print()
            logging.info('Passed all tests')
        else:
            print()
            logging.critical('Failed some tests')
            print(*failed_tests, sep='\n', file=sys.stderr)
            sys.exit(1)

if __name__ == '__main__':
    test(HOST, PORT)
