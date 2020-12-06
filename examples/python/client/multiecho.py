import os
import multiprocessing


loop = True

def callback(i):
    while loop:
        os.system('python3 client_clear.py')

#executor = ThreadPoolExecutor(max_workers=10)
#a = executor.submit(callback)

pool = multiprocessing.Pool(processes=20)
pool.map(callback, range(20))
