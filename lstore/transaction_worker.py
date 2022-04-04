from lstore.table import Table, Record
from lstore.index import Index
import threading
from time import sleep

class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions = []):
        self.stats = []
        self.transactions = []
        self.result = 0
        self.t = None
        pass

    """
    Appends t to transactions
    """
    def add_transaction(self, x):
        self.transactions.append(x)

    """
    Runs all transaction as a thread
    """
    def run(self):
        # here you need to create a thread and call __run
        self.t = threading.Thread(target=self.__run, daemon=False)
        self.t.start()
        sleep(10)
        # self.__run()
    
    """
    Waits for the worker to finish
    """
    def join(self):
        self.t.join()

    def __run(self):
        for transaction in self.transactions:
            # each transaction returns True if committed or False if aborted
            self.stats.append(transaction.run())
        # stores the number of transactions that committed
        self.result = len(list(filter(lambda x: x, self.stats)))
