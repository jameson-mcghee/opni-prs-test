import asyncio
import requests
import subprocess
import time

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from threading import Thread

from opni_nats import NatsWrapper

kube_fname = "kube.yaml"
logfile = "test.log"

nw = NatsWrapper()

t = None

foundlog = False

def test():
    start_process("killall kubectl")

    log = open("test.log")
    logdata = log.read()
    log.close()

    k1 = start_process("kubectl --kubeconfig kube.yaml -n opni-demo port-forward svc/payload-receiver-service 8080:80 &")
    k2 = start_process("kubectl --kubeconfig kube.yaml -n opni-demo port-forward svc/nats-client 4222:4222 &")

    time.sleep(5)

    # nats subscribe
    mainloop(logdata)

    start_time = time.time()
    while time.time() - start_time < 10:
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
    while time.time() - start_time < 20:
        continue

    loop = asyncio.get_event_loop()
    loop.stop()

    start_process("killall kubectl")

    assert foundlog == True

async def consume_logs(logdata):
    async def subscribe_handler(msg):
        payload_data = msg.data.decode()
        print(payload_data + '\n')
        if "start worker process 32" in payload_data:
            global foundlog 
            foundlog = True

    await nw.subscribe(
        nats_subject="raw_logs",
        nats_queue="workers",
        payload_queue="mask_logs",
        subscribe_handler=subscribe_handler,
    )

async def init_nats():
    print("Attempting to connect to NATS")
    await nw.connect()


def start_background_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def mainloop(logdata) -> None:
    loop = asyncio.get_event_loop()
    nats_consumer_coroutine = consume_logs(logdata)

    t = Thread(target=start_background_loop, args=(loop,), daemon=True)
    t.start()

    task = asyncio.run_coroutine_threadsafe(init_nats(), loop)
    task = asyncio.run_coroutine_threadsafe(nats_consumer_coroutine, loop)



def start_process(command):
    try:
        return subprocess.run(command, shell=True)
    except subprocess.CalledProcessError as e:
        return None