import asyncio


class MockInstrument:
    """ A Mock Node Executor useful for testing and simulation"""

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.delay = kwargs.get("delay", 0)

    async def __call__(self, **params):
        delay = ""
        if self.delay:
            await asyncio.sleep(self.delay)
            delay = " with delay of {} seconds".format(self.delay)
        return f"Completed {self.__class__.__name__} with {len(self.kwargs)} kwargs and {len(params)} parameters{delay}"
