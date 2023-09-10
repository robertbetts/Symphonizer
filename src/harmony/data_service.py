from typing import Optional, Dict, NoReturn


class DataService:
    def __init__(self):
        pass

    async def set_process(self, process: Dict) -> NoReturn:
        """
        :param process: Dict, process info and optionally related steps
        :return: None
        """
        pass

    async def get_process(self, process_id: str) -> Optional[Dict]:
        """
        :param process_id:
        :return: Dict with process and related steps
        """
        pass