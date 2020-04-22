import multiprocessing
from typing import Generator
from typing import List
from typing import TypeVar
from typing import Callable
from typing import Union
from logging import Logger
from typing import Iterable
import multiprocessing_logging

T = TypeVar('T')


class Composed:
    def __init__(self, functions: List[Callable[[T], T]]):
        """
        A helper class for composing a list of functions with the same input and return type, such that they become a
        single callable object that applies consecutive transformations. Beware: because of Multiprocessing internals,
        local procedures will not work (since they can't be pickled).
        :param functions: A list of callable objects such that they all share the same input and return type.
        """
        self.functions = functions

    def __call__(self, arg: T) -> T:
        """
        Consecutively applies a series of transformations to the input argument.
        :param arg: It can be any object.
        :return: The resulting transformation.
        """
        for f in self.functions:
            arg = f(arg)
        return arg


class PipelineLogger:
    def __init__(self, logger: Logger):
        self.logger = logger
        multiprocessing_logging.install_mp_handler()

    def log(self, log: str):
        self.logger.info(log)


class Pipeline:
    def __init__(self, input_generator: Generator[T, None, None], processors: List[Callable[[T], T]],
                 output_procedure: Callable[[Iterable[T]], None], batch_size: int = 1000, parallel: bool = True,
                 logger: Union[PipelineLogger, None] = None, log_every_iter: int = 10000):
        """
        A simple class for parallelizing a set of transformations that are consecutively applied to a stream of data.
        Notice that Multiprocessing's parallel map cannot work with generators, which makes it not usable when our data
        is to big to fit in memory.
        It is assumed that the transformations are independently applied to each object (ie. map-like).
        The output is ordered and deterministic (ie. in parallel the result is exactly equal to the one that would be
        obtained if the aplications
        If the application is severly I/O bound instead of CPU bound or determinism is not a concern, this class may not
        be the best option.
        The parallelization strategy is the following:
            - stream_generator will be used to generate a batch of objects that will be stored in memory.
            - When the batch is ready, a parallel map will be applied to the batch, while stream_generator prepares the
            next batch running in background.
            - Once the parallel map has been applied, the results are output in background.
        :param input_generator: A generator that yields objects. Typically, it will involve some I/O.
        :param processors: A list of callable objects (functions or objects with the __call__ method) that
        :param output_procedure: A callable object or function that receives an iterable collection of objects and
        outputs it (eg. writes it to a file).
        :param batch_size: The number of elements that will be stored in memory each time the processors are applied.
        Ideally, it should be as big as possible such that no out of memory errors are caused.
        :param parallel: Whether to run the pipeline in parallel. By default, set to True.
        """
        self.input_generator = self.batch_generator(input_generator, batch_size)
        if parallel:
            self.input_generator = self.background_generator(self.input_generator)
        self.processor = Composed(processors)
        self.processors = processors
        self.output_procedure = output_procedure
        self.parallel = parallel
        self.logger = logger
        self.log_every_iter = log_every_iter
        self.log_queue = multiprocessing.Queue()
        self.done = False

    def run(self):
        """
        Runs the pipeline with the aforementioned parallelization strategy if parallel is set to True. Otherwise, the
        pipeline is executed sequentially.
        :return:
        """
        assert not self.done
        pool = multiprocessing.Pool(multiprocessing.cpu_count())
        p_init = False
        for idx, batch in enumerate(self.input_generator):
            res = pool.map(self.processor, batch) if self.parallel else list(map(self.processor, batch))
            if self.parallel:
                if p_init:
                    p.join()
                if self.logger is not None:
                    if idx % self.log_every_iter == 0:
                        self.logger.logger.info(f'{self.__class__.__name__}: Processed batch {idx+1}')
                p = multiprocessing.Process(target=self.output_procedure, args=(res,))
                p.start()
                p_init = True
            else:
                self.output_procedure(res)

        if self.parallel:
            p.join()
        self.done = True

    @staticmethod
    def background_generator(generator: Generator[T, None, None]) -> Generator[T, None, None]:
        """
        Credits to https://stackoverflow.com/questions/49185891/make-python-generator-run-in-background
        The new generator will be preparing the next value to be yielded in background.
        :param generator: Generator that will be run in background.
        :return: The new generator.
        """
        def _bg_gen(gen, conn):
            while conn.recv():
                try:
                    conn.send(next(gen))
                except StopIteration:
                    conn.send(StopIteration)
                    return

        parent_conn, child_conn = multiprocessing.Pipe()
        p = multiprocessing.Process(target=_bg_gen, args=(generator, child_conn))
        p.start()
        parent_conn.send(True)
        while True:
            parent_conn.send(True)
            x = parent_conn.recv()
            if x is StopIteration:
                return
            else:
                yield x

    @staticmethod
    def batch_generator(gen: Generator[T, None, None], batch_size: int) -> Generator[List[T], None, None]:
        """
        A procedure for batching generators into lists that will be stored in memory. It is needed for applying a
        parallel map to the objects of the generators, batch by batch.
        :param gen: Generator to be batched.
        :param batch_size: Number of elements that will be simultaneously stored in memory. Ideally, it should be as
        big as possible as long as it doesn't generate a memory error (or it consumes too much memory for the
        requirements of the application).
        :return: A generator of lists of the original type.
        """
        while True:
            batch = []
            empty = True
            for idx, e in enumerate(gen):
                empty = False
                batch.append(e)
                if idx == batch_size - 1:
                    break
            if empty:
                break
            yield batch
