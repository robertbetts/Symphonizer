# Symphonizer
## Task Orchestration using Directed Acyclic Graphs

[![codecov](https://codecov.io/gh/robertbetts/nuropb/branch/main/graph/badge.svg?token=DVSBZY794D)](https://codecov.io/gh/robertbetts/Symphonizer)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![CodeFactor](https://www.codefactor.io/repository/github/robertbetts/symphonizer/badge)](https://www.codefactor.io/repository/github/robertbetts/symphonizer)
[![License: Apache 2.0](https://img.shields.io/pypi/l/giteo)](https://www.apache.org/licenses/LICENSE-2.0.txt)

Orchestrate anything with low overhead, fast, synchronized scheduling. Symphonizer is very well suited to orchestrating 
distributed API requests and dependency ordered tasks. This makes Symphonizer well suited in orchestrating machine learning
for model execution, LLVM agent chaining, and any other processes/tasks that can be represented as a directed acyclic graph. 

**Ideal use cases include for Symphonizer:**
* Idempotent, ordered, processes and flows
* Remote API requests
* Orchestrating calls to hybrid cloud, Lambda and Serverless Functions 
* Machine learning and model execution
* LLVM agent chaining
* Any dependency ordered API requests

**Use cases to avoid:**
* Distributed transactions with ACID guarantees
* Data pipelines with large data payloads
* Symphonizer is NOT a workflow engine
* Symphonizer is NOT a distributed transaction coordinator
* Symphonizer is NOT a transaction database

**Use cases that may require consideration before using Symphonizer:**
* Distributed transactions with eventual consistency
* Very long running processes - Hours, Days

## Symphonizer in the wild
Symphonizer was initially developed to facilitate the orchestration of autonomous applications in an unstable
distributed system and has subsequently found a nice home in orchestrating machine LLM agents and prompt 
engineering.

## Symphonizer Model
**DAGNote**: Hashable -> Is a Graph Node with a JSON serializable payload. 

**NodeRunner**: Callable[..., Any] -> Is a Function or callable class that takes a DAGNote payload as an argument.
- NodeRunners are most effective when their actions are idempotent.
- NodeRunners are intended to serve as execution proxies having low compute overhead. 
- An instance of a node runner can only be executed once and is then immutable, this is to ensure idempotency. 
  In order to retry or execute a NodeRunner again, a new instance is required. the NodeRunner.retry_task() method 
  will clone a new child NodeRunner. 

**Composition**: object -> Is a class instantiated with a Dict[Hashable, Set[Hashable]] that represents 
a directed acyclic graph. Read further in the graphlib standard library documentation.
- Each DAGNote's NodeRunner is executed in topological order until all nodes have been executed.
- All execution is synchronized and asynchronously run in memory. 

**Perform**: object -> Is a class that orchestrates the execution of a Compositions. Perform is instantiated with a
high water and low water mark. When the number of concurrent running Compositions reach the high water mark, new 
composition execution is blocked until the number of running Compositions falls below the low water mark. During
this time all new Compositions are queued until the low water mark is reached. 

## Getting started

```
pip install symphonizer
```

## Example

Here is a basic end to end example of Symphonizer. This example is also available in the examples directory.
```python  
import asyncio
import time
import random

from symphonizer.node_runner import NodeRunner
from symphonizer.composition import Composition, DAGNote
from symphonizer.perform import Perform
```
With this example we will customise the Composition class in and add a sleep time to the processing of each node.  
```python
class Compose(Composition):
    @classmethod
    def configure_node_runner(cls, node: DAGNote):
        """ Configure the node runner as appropriate for the DAGNote, the flexibility of this method allows for
        different node runners to be used for different nodes in the DAG or alternatively the same node runner
        where the execute method is configured differently for different nodes in the DAG.
        """
        async def execute(**params):
            sleep_time = random.uniform(0.001, 0.1)
            # print(f"Processing node {node}, sleep for {sleep_time}")
            await asyncio.sleep(sleep_time)

        return NodeRunner().prepare(node=node).run(execute)
```
Next is the main body of the example which you would ammend to suit your needs. 
```python
async def main():
    sample_graph = {
        DAGNote("D"): {DAGNote("B"), DAGNote("C")},
        DAGNote("C"): {DAGNote("A")},
        DAGNote("B"): {DAGNote("A")}
    }
    dag_count_target = 1000
    dag_count_completed = 0
    dag_tracker = {}
    test_stop_future = asyncio.Future()
    perform = Perform()
    start_time = time.time()

    def scheduler_done_cb(instance, status, error=None, elapsed_time: float = 0):
        nonlocal dag_count_completed
        dag_tracker.pop(instance.instance_id, None)
        dag_count_completed += 1
        if dag_count_completed == dag_count_target:
            test_stop_future.set_result(None)

    async def schedule_dag():
        dag = Compose(
            sample_graph,
            schedule_done_cb=scheduler_done_cb,
        )
        asyncio.create_task(perform.add(dag))
        await asyncio.sleep(0)

    print("Starting to schedule DAGs")

    _ = list([await schedule_dag() for _ in range(dag_count_target)])

    print(f"added {dag_count_target} DAGs to perform")
    await asyncio.sleep(0.001)
    await test_stop_future
    end_time = time.time()
    print(f"All DAGs processed in {end_time-start_time} seconds")
    print(f"Average DAG processing time {round((end_time-start_time)/dag_count_target, 4)} seconds")

if __name__ == "__main__":
    asyncio.run(main())
```
