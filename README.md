# pipelib

`pipelib` is a Python library for parallelizing map-like functions inspired by MapReduce, but very simple. Full
disclaimer: I have published this small package because personally I found it helpful, but do not expect any guarantees
or maintenance. I am sure that there are tons of libraries better than `pipelib`, but this approach worked for me.

### Install

`pipelib` supports Python >= 3.6.

```
pip install pipelib
```


### Use case
Parallelize map-like functions that need to be applied to generators (instead of lists), since data do not fit in
memory. In addition, using callable objects instead of functions is allowed, even if the objects are not pickable.
Process-safe logging is provided thanks to the `multiprocessing-logging` library.

### Usage

The code is documented and there is one example in the ```examples/``` directory. For a real-world usage, see
<https://github.com/TeMU-BSC/CorpusCleaner>.

```
pipeline = Pipeline(streamers=streamers,  # List of generators
                    mappers_factory=mappers_factory,  # Function that returns a list of functions/callables that will be
                                                      # consecutively applied to each element in each generator
                    output_reducer=output_reducer,  # Function that receives batches of the objects processed by the
                                                    # mappers; typically, it will write them in disk.
                    batch_size=batch_size,  # Number of objects yielded from the generators that will be simultaneously
                                            # instantiated in memory
                    parallel=True,
                    logger=logger,
                    log_every_iter=1)
pipeline.run()
```