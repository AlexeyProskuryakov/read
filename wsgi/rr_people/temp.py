# coding=utf-8
import time
import sys
import os
from multiprocessing import Process, freeze_support


def info(title):
    if hasattr(os, 'getppid'):  # only available on Unix
        print('{0}:\tPID={1} PPID={2}'.format(title, os.getpid(), os.getppid()))
    else:
        print('{0}:\tPID={1}'.format(title, os.getpid()))


from subprocess import check_output


def get_pids():
    return map(int, check_output(["pidof", "python"]).split())


if __name__ == '__main__':
    freeze_support()

    from multiprocessing import Process
    def f():
        while 1:
            print "foo"
            import time
            time.sleep(1)
            print get_pids()

    pr = Process(target=f)
    pr.start()

    nproc = len(sys.argv) > 1 and int(sys.argv[1]) or 3
    print 'число дочерних процессов ', nproc
    info('родительский процесс')
    procs = []
    for i in range(nproc):
        procs.append(Process(target=f,))
    for i in range(nproc):
        procs[i].start()
    for i in range(nproc):
        procs[i].join()
    print('завершается родительский процесс')
