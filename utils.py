# -*- coding: utf-8 -*-

import threading
import queue
import time
import uuid


class Channel(object):
    def __init__(self, maxsize=1):
        super(Channel, self).__init__()
        self.client2server = queue.Queue(maxsize)
        self.server2client = queue.Queue(maxsize)

    @staticmethod
    def put(channel, item, timeout=0):
        now = time.time()
        while True:
            if timeout > 0:
                if time.time() - now > timeout:
                    raise TimeoutError('发送数据超时')
            try:
                channel.put(item, block=False)
                return
            except Exception as _:
                time.sleep(0.1)

    @staticmethod
    def get(channel, timeout=0):
        now = time.time()
        while True:
            if timeout > 0:
                if time.time() - now > timeout:
                    raise TimeoutError('获取数据超时')
            try:
                return channel.get(block=False)
            except Exception as _:
                time.sleep(0.1)

    def client_put(self, item, timeout=0):
        self.put(self.client2server, item, timeout=timeout)

    def client_get(self, timeout=0):
        return self.get(self.server2client, timeout=timeout)

    def server_put(self, item, timeout=0):
        self.put(self.server2client, item, timeout=timeout)

    def server_get(self, timeout=0):
        return self.get(self.client2server, timeout=timeout)


class ChannelProxy(object):
    def put(self):
        pass

    def get(self):
        pass


class AsyncWork(threading.Thread):
    def __init__(self):
        super(AsyncWork, self).__init__()
        self.task_queue = queue.Queue()
        self.working_task = []
        self.channels = {}
        self.max_thread_num = 10

    def async_call(self, function, callback, *args, **kwargs):
        task_id = uuid.uuid1()
        channel = Channel()
        channel_client = ChannelProxy()
        channel_server = ChannelProxy()
        channel_client.put = channel.client_put
        channel_client.get = channel.client_get
        channel_server.put = channel.server_put
        channel_server.get = channel.server_get
        self.channels[task_id] = channel_client
        kwargs['channel'] = channel_server
        self.task_queue.put({'function': function, 'callback': callback, 'args': args, 'kwargs': kwargs})
        return task_id

    def callback_proxy(self, callback, func, args, kwargs):
        try:
            callback(func(*args, **kwargs))
        except Exception as e:
            callback(e)

    def get_task_channel(self, task_id):
        return self.channels[task_id]

    def run(self):
        while True:
            while not self.task_queue.empty() and len(self.working_task) < self.max_thread_num:
                task = self.task_queue.get()
                function = task.get('function')
                callback = task.get('callback')
                args = task.get('args')
                kwargs = task.get('kwargs')
                t = threading.Thread(target=self.callback_proxy, args=(callback, function, args, kwargs))
                t.daemon = True
                t.start()
                self.working_task.append(t)
                self.task_queue.task_done()
            else:
                for task in self.working_task:
                    if not task.isAlive():
                        self.working_task.remove(task)


if __name__ == '__main__':
    def fun(channel):
        channel.put('100')
        input = channel.get()
        return input

    def call_back(result):
        print("result", result)

    aw = AsyncWork()
    aw.start()
    task_id = aw.async_call(fun, call_back)
    c_channel = aw.get_task_channel(task_id)
    print(c_channel.get())
    c_channel.put("hahahhah")
