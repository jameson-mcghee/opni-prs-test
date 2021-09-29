import asyncio
import json
import subprocess
import time
from queue import Queue
from subprocess import PIPE, Popen
from threading import Thread

import requests
from faker import Faker
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout
from opni_nats import NatsWrapper

kube_fname = "kube.yaml"
nw = NatsWrapper()
fake = Faker()
port_forward_command = "kubectl --kubeconfig kube.yaml -n opni-demo port-forward "
json_payload = []

def test_prs_happy_path():
    
    # This test is to verify the happy path functionality of the Payload Receiver Service (PRS). 
    # In this test, we are verifying that each of the following fields are successfully added to a log submitted to the PRS.
        # time
        # window_dt
        # window_start_time_ns
        # _id

    tr_queue = Queue()

    start_process("killall kubectl")
    
    log_data = ('{"log": {"0":"' + fake.sentence(10) + '"}}')

    kube_nats_process = Popen([port_forward_command + "svc/nats-client 4222:4222"], shell=True)
    kube_payload_process = Popen([port_forward_command + "svc/payload-receiver-service 8080:80"], stdout=PIPE, shell=True)
        
    i = 0
    while i <= 5: 
        if kube_payload_process.poll != None and kube_nats_process.poll != None :
            time.sleep(1)
            i += 1
        else :
            raise Exception('kube_payload_process and/or kube_nats_process are not running')

    # nats subscribe
    t = subscribe(tr_queue, log_data)
    
    wait_for_seconds(2)

    print('Sending Dataset')
    r = requests.post('http://localhost:8080',
            data=log_data,
            verify=False)
        
    if len(r.content) != 0:
        print(('Request Content: '), r.content)
        print(('Request Headers: '), r.headers)
        print(('Request Status Code'), r.status_code)
        bad_content = '{"detail":"Something wrong with request'
        content = r.content.decode("utf-8")
        if bad_content in content:
            raise Exception('Bad Request sent to API')
        if r.status_code != 200:
            raise Exception('Bad Request sent to API')
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    kube_payload_process.terminate
    kube_payload_process.wait
    kube_nats_process.terminate
    kube_nats_process.wait
    assert kube_payload_process.returncode == None
    assert kube_nats_process.returncode == None

    global json_payload
    assert json_payload["time"]["0"] != None
    assert json_payload["window_dt"]["0"] > 1632792570000
    assert json_payload["window_start_time_ns"]["0"] > 1632792570000000000
    assert (int(json_payload["_id"]["0"])) > 16327923057044440000000000000000000

def test_prs_unique_id():
    
    # This test is to verify the each log submitted to the Payload Receiver Service (PRS) is unique. 

    tr_queue = Queue()

    start_process("killall kubectl")
    
    global json_payload
    log_data = ('{"log": {"0":"' + fake.sentence(10) + '"}}')

    kube_nats_process = Popen([port_forward_command + "svc/nats-client 4222:4222"], shell=True)
    kube_payload_process = Popen([port_forward_command + "svc/payload-receiver-service 8080:80"], stdout=PIPE, shell=True)
        
    i = 0
    while i <= 5: 
        if kube_payload_process.poll != None and kube_nats_process.poll != None :
            time.sleep(1)
            i += 1
        else :
            raise Exception('kube_payload_process and/or kube_nats_process are not running')

    # nats subscribe
    t = subscribe(tr_queue, log_data)
    
    wait_for_seconds(2)

    print('Sending Dataset 1')
    r = requests.post('http://localhost:8080',
            data=log_data,
            verify=False)
    id_1 = json_payload["_id"]["0"]
    print('First ID:', id_1)

    if len(r.content) != 0:
        print(('Request Content: '), r.content)
        print(('Request Headers: '), r.headers)
        print(('Request Status Code'), r.status_code)
        bad_content = '{"detail":"Something wrong with request'
        content = r.content.decode("utf-8")
        if bad_content in content:
            raise Exception('Bad Request sent to API')
        if r.status_code != 200:
            raise Exception('Bad Request sent to API')

    wait_for_seconds(2)

    print('Sending Dataset 2')
    r2 = requests.post('http://localhost:8080',
            data=log_data,
            verify=False)
    id_2 = json_payload["_id"]["0"]
    print('Second ID:', id_2)
        
    if len(r2.content) != 0:
        print(('Request Content: '), r2.content)
        print(('Request Headers: '), r2.headers)
        print(('Request Status Code'), r2.status_code)
        bad_content = '{"detail":"Something wrong with request'
        content = r2.content.decode("utf-8")
        if bad_content in content:
            raise Exception('Bad Request sent to API')
        if r2.status_code != 200:
            raise Exception('Bad Request sent to API')
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    kube_payload_process.terminate
    kube_payload_process.wait
    kube_nats_process.terminate
    kube_nats_process.wait
    assert kube_payload_process.returncode == None
    assert kube_nats_process.returncode == None

    assert id_2 != id_1

def test_prs_large_log():
    
    # This test is to verify the Payload Receiver Service (PRS) can handle very large payloads with mulitple logs.

    tr_queue = Queue()

    start_process("killall kubectl")
    
    log_data = '[{"log": {"0": "' + fake.sentence(100000) + '"}},\
    {"log": {"1": "' + fake.sentence(100000) + '"}},\
    {"log": {"2": "' + fake.sentence(100000) + '"}},\
    {"log": {"3": "' + fake.sentence(100000) + '"}},\
    {"log": {"4": "' + fake.sentence(100000) + '"}},\
    {"log": {"5": "' + fake.sentence(100000) + '"}},\
    {"log": {"6": "' + fake.sentence(100000) + '"}}]';

    kube_nats_process = Popen([port_forward_command + "svc/nats-client 4222:4222"], shell=True)
    kube_payload_process = Popen([port_forward_command + "svc/payload-receiver-service 8080:80"], stdout=PIPE, shell=True)
        
    i = 0
    while i <= 5: 
        if kube_payload_process.poll != None and kube_nats_process.poll != None :
            time.sleep(1)
            i += 1
        else :
            raise Exception('kube_payload_process and/or kube_nats_process are not running')

    # nats subscribe
    t = subscribe(tr_queue, log_data)
    
    wait_for_seconds(2)

    print('Sending Dataset')
    r = requests.post('http://localhost:8080',
            data=log_data,
            verify=False)
        
    if len(r.content) != 0:
        print(('Request Content: '), r.content)
        print(('Request Headers: '), r.headers)
        print(('Request Status Code'), r.status_code)
        bad_content = '{"detail":"Something wrong with request'
        content = r.content.decode("utf-8")
        if bad_content in content:
            raise Exception('Bad Request sent to API')
        if r.status_code != 200:
            raise Exception('Bad Request sent to API')
    
    wait_for_seconds(2)

    loop = asyncio.get_event_loop()
    loop.stop()

    kube_payload_process.terminate
    kube_payload_process.wait
    kube_nats_process.terminate
    kube_nats_process.wait
    assert kube_payload_process.returncode == None
    assert kube_nats_process.returncode == None

    global json_payload
    assert json_payload["time"]["0"] != None
    assert json_payload["window_dt"]["0"] > 1632792570000
    assert json_payload["window_start_time_ns"]["0"] > 1632792570000000000
    assert (int(json_payload["_id"]["0"])) > 16327923057044440000000000000000000
    print(json_payload["_id"])


def wait_for_seconds(seconds):
    start_time = time.time()
    while time.time() - start_time < seconds:
        continue


def check_logs(incoming, expected):
    if expected in incoming:
        return True


async def consume_logs(trqueue, logdata):
    async def subscribe_handler(msg):
        payload_data = msg.data.decode()
        if check_logs(payload_data, logdata):
            trqueue.put(True)

        payload = json.loads(payload_data)
        global json_payload
        json_payload = payload

    await nw.subscribe(
        nats_subject="raw_logs",
        subscribe_handler=subscribe_handler,
    )


async def init_nats():
    print("Attempting to connect to NATS")
    await nw.connect()
    assert nw.connect().__init__

def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def subscribe(trqueue, logdata):

    loop = asyncio.get_event_loop()
    nats_consumer_coroutine = consume_logs(trqueue, logdata)

    t = Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()

    asyncio.run_coroutine_threadsafe(init_nats(), loop)
    asyncio.run_coroutine_threadsafe(nats_consumer_coroutine, loop)

    return t


def start_process(command):
    try:
        return subprocess.run(command, shell=True)
    except subprocess.CalledProcessError as e:
        return None
