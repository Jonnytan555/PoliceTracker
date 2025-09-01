from abc import ABC, abstractmethod

class Observer(ABC):
    @abstractmethod
    def update(self, downloaded_files, files_to_download, message: str = ''):
        ...

class Subject(ABC):
    @abstractmethod
    def add_progress_reporter(self, observer: Observer): ...
    @abstractmethod
    def add_complete_reporter(self, observer: Observer): ...
    @abstractmethod
    def add_error_reporter(self, observer: Observer): ...
