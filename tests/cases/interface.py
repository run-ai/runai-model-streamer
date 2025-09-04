from abc import ABC, abstractmethod


class ObjectStoreBackend(ABC):

    @abstractmethod
    def __init__(self):
        raise NotImplementedError

    @abstractmethod
    def wait_for_startup(self, timeout: int = 30):
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, bucket_name: str, directory: str, file_path: str):
        raise NotImplementedError
