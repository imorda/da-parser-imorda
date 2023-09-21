from __future__ import annotations

import concurrent.futures
import itertools
import random as rd
import threading
import time
import traceback
from concurrent.futures import *
from typing import Callable, List, Any

import requests


class Crawler:
    def __init__(self, parser: Callable[[Crawler, int, Any, bytes], None], proxies: List[str], cookies: dict,
                 headers: dict, result: Any):
        self.executor = ThreadPoolExecutor()
        self.parser = parser
        self.proxies = proxies
        self.futures = []
        self.cookies = cookies
        self.headers = headers
        self.result = result
        self.waiting_for_input = False
        self.success_requests = itertools.count()
        self.lock = threading.Lock()

    def enqueue(self, url, depth=0, context=None):
        def worker():
            retries = 20
            response = ""
            response_raw = bytes()
            while retries:
                chosen_proxy = rd.choice(self.proxies)
                proxies = {
                    'http': chosen_proxy,
                }
                print(f"[Crawler] try {url} with proxy {chosen_proxy}...")
                try:
                    while self.waiting_for_input:
                        time.sleep(0.5)
                    r = requests.get(url=url, allow_redirects=True, proxies=proxies,
                                     cookies=self.cookies, headers=self.headers)
                    print(f"[Crawler] Got {r.status_code} on {url}")
                    if r.status_code == 200:
                        try:
                            response = r.content.decode('utf-8')
                            response_raw = r.content
                            if response.find("похожи на автоматические") == -1:
                                break
                            else:
                                print(f"[Crawler] got CAPTCHA on {url}")
                                if retries < 19:
                                    self.waiting_for_input = True
                                    with self.lock:
                                        if self.waiting_for_input:
                                            self.cookies["spravka"] = input(f"Enter new SPRAVKA, pls: ").strip()
                                            self.waiting_for_input = False
                        except UnicodeDecodeError:
                            break
                    elif 500 <= r.status_code:
                        pass
                    elif 200 <= r.status_code < 300:
                        return
                except Exception as e:
                    print(f"[Crawler] ERROR {url}: {e}")
                retries -= 1
            success_requests = next(self.success_requests)
            print(f"[Crawler] successful requests: {success_requests}, total: {len(self.futures)}")
            self.parser(self, depth, context, response_raw)

        self.futures.append(self.executor.submit(worker))

    def join(self):
        futures = []
        while futures != self.futures:
            futures = self.futures.copy()
            for future in concurrent.futures.as_completed(futures):
                if future.exception() is not None:
                    try:
                        raise future.exception()
                    except Exception:
                        traceback.print_exc()
        self.futures = []

    def __del__(self):
        self.executor.shutdown()
