import json
from enum import Enum
from json import JSONDecodeError


class MessageEnum(Enum):
    DATA = 'data'
    CONTROL = 'control'


class MessageError(Exception):
    pass


class Message:
    def __init__(self, type: MessageEnum, src=None, src_id=None, payload=None):
        self.type = type
        self.src = src
        self.src_id = src_id
        self.payload = payload

    def __repr__(self):
        return str(self.__dict__)

    @property
    def is_control(self):
        return self.type == MessageEnum.CONTROL.value

    @property
    def is_data(self):
        return self.type == MessageEnum.DATA.value

    def data_ready(self):
        return self.is_control and self.payload == 'ready'

    @classmethod
    def from_bytes(cls, read: bytes):
        try:
            data = json.loads(read)
            return Message(**data)
        except JSONDecodeError:
            raise MessageError

    @classmethod
    def create_data(cls, *args, **kwargs):
        return cls(type=MessageEnum.DATA.value, *args, **kwargs)

    def dump(self):
        return json.dumps(self.__dict__)