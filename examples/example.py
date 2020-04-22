from pipelib import Pipeline
from pipelib import PipelineLogger
import os
import filecmp
import time
import string
from typing import Iterable
from typing import Generator
from typing import cast
import multiprocessing
import logging
import shutil

DATA_DIR = 'data'
RES_DIR = 'res'
os.chdir(os.path.dirname(os.path.abspath(__file__)))


def prepare_example(data_dir: str):
    os.makedirs(data_dir)
    i = 0
    for c in string.ascii_lowercase:
        path = os.path.join(data_dir, c + '.txt')
        with open(path, 'w') as f:
            for j in range(1000):
                f.write(str(i + j) + '\n')
            i += j


def input_generator(cost: int = 0) -> Iterable[int]:
    data_dir = DATA_DIR
    for c in string.ascii_lowercase:
        path = os.path.join(data_dir, c + '.txt')
        with open(path, 'r') as f:
            for i in f.readlines():
                time.sleep(cost*0.0001)
                yield int(i.strip())


def input_list(cost: int = 0) -> Iterable[int]:
    data_dir = DATA_DIR
    res = []
    for c in string.ascii_lowercase:
        path = os.path.join(data_dir, c + '.txt')
        with open(path, 'r') as f:
            for i in f.readlines():
                time.sleep(cost * 0.0001)
                res.append(int(i.strip()))
    return res


def p1(i: int) -> int:
    cost = 1
    time.sleep(cost * 0.0001)
    return i


class P2:
    def __init__(self, cost: int, logger: PipelineLogger = None):
        self.cost = cost
        self.logger = logger

    def __call__(self, i: int) -> int:
        time.sleep(self.cost * 0.0001)
        if self.logger is not None and i % 10000 == 0:
            self.logger.log(f'{self.__class__.__name__}: processed the number {i}')
        return i


def p3(i: int) -> int:
    cost = 3
    time.sleep(cost * 0.0001)
    return i


class Output:
    def __init__(self, file: str, cost: int):
        self.fd = open(file, 'w')
        self.cost = cost

    def __call__(self, res: Iterable[int]):
        for e in res:
            time.sleep(self.cost * 0.0001)
            self.fd.write(str(e) + '\n')
        self.fd.flush()

    def __del__(self):
        self.fd.close()


def init_logger() -> logging.Logger:
    logging.basicConfig(filename='pipe.log', level=logging.INFO)
    logger = logging.getLogger('')
    logger.addHandler(logging.StreamHandler())

    return logger


def run():
    if not os.path.exists(DATA_DIR):
        os.makedirs(RES_DIR)
    prepare_example(DATA_DIR)

    for case in ['Equal', 'IO-bound', 'CPU-bound']:
        print(case)
        if case == 'Equal':
            io_cost = 2
            cpu_cost = 0
        elif case == 'IO-bound':
            io_cost = 10
            cpu_cost = 0
        else:
            io_cost = 0
            cpu_cost = 2
        t0_input = time.time()
        res = input_list(io_cost)
        t1_input = time.time()
        t0_cpu = time.time()
        res = [p3(P2(cpu_cost)(p1(e))) for e in res]
        t1_cpu = time.time()
        t0_output = time.time()
        out = Output(os.path.join(RES_DIR, 'seq.txt'), io_cost)
        out(res)
        t1_output = time.time()
        io = t1_input - t0_input + t1_output - t0_output
        cpu = t1_cpu - t0_cpu
        total = io + cpu
        print('Vanilla Sequential')
        print(f'IO: {io}s ({100*io/total:.2f}%)')
        print(f'CPU: {cpu}s ({100*cpu/total:.2f}%)')
        print(f'Total {total:.2f}s')
        print()

        t0_pipeseq = time.time()
        pipeline = Pipeline(input_generator=cast(Generator, input_generator(io_cost)),
                            processors=[p1, P2(cpu_cost), p3],
                            output_procedure=Output(os.path.join(RES_DIR, 'pipeseq.txt'), io_cost), batch_size=1000,
                            parallel=False, logger=None)
        pipeline.run()
        t1_pipeseq = time.time()
        correct = filecmp.cmp(os.path.join(RES_DIR, 'seq.txt'), os.path.join(RES_DIR, 'pipeseq.txt'))
        print(f'pipelib sequential: {t1_pipeseq - t0_pipeseq:.2f}s')
        print(f'pipelib sequential is {"CORRECT" if correct  else "INCORRECT"}')
        print()

        t0_pipepar = time.time()

        pipeline = Pipeline(input_generator=cast(Generator, input_generator(io_cost)),
                            processors=[p1, P2(cpu_cost), p3],
                            output_procedure=Output(os.path.join(RES_DIR, 'pipepar.txt'), io_cost), batch_size=1000,
                            parallel=True, logger=None)
        pipeline.run()

        t1_pipepar = time.time()
        correct = filecmp.cmp(os.path.join(RES_DIR, 'seq.txt'), os.path.join(RES_DIR, 'pipepar.txt'))
        print(f'pipelib parallel with {multiprocessing.cpu_count()} cores: {t1_pipepar - t0_pipepar:.2f}s')
        print(f'pipelib parallel is {"CORRECT" if correct  else "INCORRECT"}')
        print()

    print('Example with logs for the pipeline and P2:')
    io_cost = 2
    cpu_cost = 0
    logger = init_logger()
    logger = PipelineLogger(logger)
    pipeline = Pipeline(input_generator=cast(Generator, input_generator(io_cost)),
                        processors=[p1, P2(cpu_cost, logger=logger), p3],
                        output_procedure=Output(os.path.join(RES_DIR, 'pipeparlogs.txt'), io_cost), batch_size=1000,
                        parallel=True, logger=logger, log_every_iter=1)
    pipeline.run()

    shutil.rmtree(DATA_DIR)
    shutil.rmtree(RES_DIR)


if __name__ == '__main__':
    run()
