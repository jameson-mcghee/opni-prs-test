import asyncio
import requests
import subprocess
import time

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers
from queue import Queue
from threading import Thread
from faker import Faker

from opni_nats import NatsWrapper

from subprocess import Popen, PIPE

kube_fname = "kube.yaml"

nw = NatsWrapper()

def test():
    tr_queue = Queue()

    start_process("killall kubectl")

    fake = Faker()
    log_data = ('{"log": "' + fake.sentence(1000) + '"}')

    port_forward_command = "kubectl --kubeconfig kube.yaml -n opni-demo port-forward "
    kube_nats_process = Popen([port_forward_command + "svc/nats-client 4222:4222"], shell=True)
    kube_payload_process = Popen([port_forward_command + "svc/payload-receiver-service 8080:80"], stdout=PIPE, shell=True)
        
    i = 0
    while i <= 5 : 
        if kube_payload_process.poll != None and kube_nats_process.poll != None :
            time.sleep(1)
            i += 1
        else :
            raise Exception('kube_payload_process and/or kube_nats_process are not running')

    # nats subscribe
    t = subscribe(tr_queue, "start worker process 32")

    wait_for_seconds(5)

    print("Sending dataset")
    r = requests.post("http://localhost:8080",
                      data=log_data,
                      verify=False)
    
    if len(r.content) != 0:
        print(("Request content: "), r.content)

    wait_for_seconds(2)
    # i = 0
    # while i <= 2 : 
    #     if r.status_code != 200 :
    #         time.sleep(1)
    #         i += 1
    #     else :
    #         break
    assert r.status_code == 200

    loop = asyncio.get_event_loop()
    loop.stop()

    kube_payload_process.kill
    kube_payload_process.wait
    kube_nats_process.kill
    kube_nats_process.wait
    assert kube_payload_process.returncode == None
    assert kube_nats_process.returncode == None

    foundlog = tr_queue.get()
    tr_queue.task_done()
    t.join()

    print(("Foundlog: "), foundlog)
    assert foundlog == True


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
        print('payload data' + payload_data + '\n')
        if check_logs(payload_data, logdata):
            trqueue.put(True)

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