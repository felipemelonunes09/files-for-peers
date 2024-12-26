from abc import ABC, abstractmethod

class ISerializable(ABC):
    @abstractmethod
    def serialize(self) -> dict[str, object]:
        pass