# cancel-token for QuarkChain services
ported from https://github.com/ethereum/asyncio-cancel-token to work with python3.5 (pypy)

## What is CancelToken?
see https://vorpus.org/blog/timeouts-and-cancellation-for-humans/

Essentially, it allows your async operations to "give up after <some arbitrary condition becomes true>". Its chaining feature allows cancelling complex service/tasks altogether.
