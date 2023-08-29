import multiprocessing
import subprocess


def run_file1():
    subprocess.run(['python', 'rates.py'])

def run_file2():
    subprocess.run(['python', 'forex.py'])

def run_file3():
    subprocess.run(['python', 'equities.py'])

def run_file4():
    subprocess.run(['python', 'credits.py'])
       
def run_file5():
    subprocess.run(['python', 'commodities.py'])


if __name__ == '__main__':
    pool = multiprocessing.Pool(processes=5)  # You can adjust the number of processes as needed

    pool.apply_async(run_file1)
    pool.apply_async(run_file2)
    pool.apply_async(run_file3)
    pool.apply_async(run_file4)
    pool.apply_async(run_file5)

    pool.close()
    pool.join()

