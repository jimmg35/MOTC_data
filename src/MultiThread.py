import time
import queue
import threading
from typing import TypeVar, Generic, List
from colorama import init, Fore, Back, Style
init(convert=True)

from .requester import Requester
from .utils import UrlBundler, Key

class Worker(threading.Thread):
    
    ''' 
        元Worker類別
    '''

    def __init__(self, queue):
        threading.Thread.__init__(self)
        self.queue = queue


class FixedRequestWorker(Worker):
    
    ''' 
        繼承Worker類別--
        用於平行API請求的Worker 
        僅適用於固定點即時資料
    '''

    def __init__(self, queue, worker_id):
        threading.Thread.__init__(self)
        super().__init__(queue)
        self.worker_id = worker_id

        # initialize basic object.
        myKey: Key = Key()
        myBundler = UrlBundler()
        self.myReq = Requester(myBundler, myKey)
        self.total_dataChunk = []

    ## 複寫Thread類別的run方法
    def run(self):
        while self.queue.qsize() > 0:
            
            # 從佇列拿取資料
            taskData = self.queue.get()
            try:
                # 對API進行請求
                dataChunk = self.myReq.getRealTimeProjectData(
                    taskData["projectKey"],
                    taskData["sensor_ids"]
                )
                self.total_dataChunk.append(dataChunk)

                print(Style.RESET_ALL + Fore.LIGHTGREEN_EX +
                    "{} fetched by worker_{} successfully!".format(
                        taskData["projectId"],
                        self.worker_id
                    ) + Style.RESET_ALL
                )
            except:
                # self.queue.put(taskData)
                print(Style.RESET_ALL + Fore.RED +
                    "{} fetched by worker_{} failed!".format(
                        taskData["projectId"],
                        self.worker_id
                    ) + Style.RESET_ALL
                )
    
    def export(self):
        return self.total_dataChunk

class FixedDbWorker(Worker):
    ''' 
        繼承Worker類別--
        用於固定點資料進入DB時
    '''

    def __init__(self, queue, worker_id, cursor):
        threading.Thread.__init__(self)
        super().__init__(queue)
        self.worker_id = worker_id
        self.cursor = cursor
    
    def run(self):
        while self.queue.qsize() > 0:
            taskData = self.queue.get()
            try:
                self.cursor.execute(taskData["query"])
                # print(Style.RESET_ALL + Fore.LIGHTGREEN_EX +
                #     "worker_{} import successfully!".format(
                #         self.worker_id
                #     ) + Style.RESET_ALL
                # )
            except:
                # print(taskData["query"])
                # self.queue.put(taskData)
                print(Style.RESET_ALL + Fore.RED +
                    "worker_{} import failed!".format(
                        self.worker_id
                    ) + Style.RESET_ALL
                )
            



T = TypeVar('T')
class WorkerCollection(Generic[T]):
    workers: List[T]

    def __init__(self):
        self.workers = []

    def add(self, worker: T) -> None:
        self.workers.append(worker)
    
    def startAll(self) -> None:
        for worker in self.workers:
            worker.start()

    def joinAll(self) -> None:
        for worker in self.workers:
            worker.join()
    
    def gatherOutput(self):
        output = []
        for worker in self.workers:
            output += worker.export()
        return output