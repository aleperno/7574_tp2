import json
from enum import Enum
from json import JSONDecodeError


class MessageEnum(Enum):
    DATA = 'data'
    CONTROL = 'control'
    READY = 'ready'
    DONE = 'done'
    EOF = 'eof'


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
        return self.is_control and self.payload == MessageEnum.READY.value

    def control_done(self):
        return self.is_control and self.payload == MessageEnum.DONE.value

    def eof(self):
        return self.is_control and self.payload == MessageEnum.EOF.value

    @classmethod
    def from_bytes(cls, read: bytes):
        try:
            data = json.loads(read)
            return Message(**data)
        except JSONDecodeError:
            raise MessageError

    @classmethod
    def create_eof(cls):
        return cls(type=MessageEnum.CONTROL.value, payload=MessageEnum.EOF.value)

    @classmethod
    def create_data(cls, *args, **kwargs):
        return cls(type=MessageEnum.DATA.value, *args, **kwargs)

    @classmethod
    def create_done(cls, src=None, src_id=None):
        return cls(src=src, src_id=src_id, type=MessageEnum.CONTROL.value, payload=MessageEnum.DONE.value)

    def dump(self):
        return json.dumps(self.__dict__)
