import asyncio
import requests
import subprocess
import time

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from queue import Queue
from threading import Thread

from opni_nats import NatsWrapper

kube_fname = "kube.yaml"
logfile = "test.log"

nw = NatsWrapper()

def test():
    tr_queue = Queue()

    start_process("killall kubectl")

    log = open("test.log")
    logdata = log.read()
    log.close()

    k1 = start_process("kubectl --kubeconfig kube.yaml -n opni-demo port-forward svc/payload-receiver-service 8080:80 &")
    k2 = start_process("kubectl --kubeconfig kube.yaml -n opni-demo port-forward svc/nats-client 4222:4222 &")

    time.sleep(5)

    # nats subscribe
    subscribe(tr_queue, "start worker process 32")

    start_time = time.time()
    while time.time() - start_time < 5:
        continue

    print("sending data")
    r = requests.post("http://localhost:8080",
                      data=logdata,
                      verify=False)
    
    if len(r.content) != 0:
        print(r.content)

    print("Status: ")
    print(r.status_code)

    start_time = time.time()
    while time.time() - start_time < 5:
        continue

    loop = asyncio.get_event_loop()
    loop.stop()

    start_process("killall kubectl")

    foundlog = tr_queue.get()
    assert foundlog == True
    tr_queue.task_done()

def check_logs(incoming, expected):
    if expected in incoming:
        return True

async def consume_logs(trqueue, logdata):
    async def subscribe_handler(msg):
        payload_data = msg.data.decode()
        print(payload_data + '\n')
        if check_logs(payload_data, logdata):
            trqueue.put(True)

    await nw.subscribe(
        nats_subject="raw_logs",
        subscribe_handler=subscribe_handler,
    )

async def init_nats():
    print("Attempting to connect to NATS")
    await nw.connect()


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def subscribe(trqueue, logdata):
    loop = asyncio.get_event_loop()
    nats_consumer_coroutine = consume_logs(trqueue, logdata)

    t = Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()

    task = asyncio.run_coroutine_threadsafe(init_nats(), loop)
    task = asyncio.run_coroutine_threadsafe(nats_consumer_coroutine, loop)



def start_process(command):
    try:
        return subprocess.run(command, shell=True)
    except subprocess.CalledProcessError as e:
        return None