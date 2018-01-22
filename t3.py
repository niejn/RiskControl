
class UserModel(Model):
    __tablename__ = "user"
    user_id = Column('user_id', INT, unique=True)
    ...
    def to_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


from sqlalchemy.orm import class_mapper


def to_dict(obj):
    return dict((col.name, getattr(obj, col.name)) \
                for col in class_mapper(obj.__class__).mapped_table.c)


class UserModel(Model):
    __tablename__ = "user"
    user_id = Column('user_id', INT, unique=True)
    ...

    def to_dict(self):
        column_name_list = [
            value[0] for value in self._sa_instance_state.attrs.items()
        ]
        return dict(
            (column_name, getattr(self, column_name, None)) \
            for column_name in column_name_list
        )


class UserModel(Model):
    __tablename__ = "user"
    user_id = Column('user_id', INT, unique=True)
    ...

    def to_dict(self):
        raise NotImplemented

        # 编写注册函数, 在 models/__init__.py 里,


from .base import (
    BaseModel,
)

from .user import (
    UserModel,
)

from .post import (
    PostModel,
)

"""
Note BaseModel 是我定义的基类, 继承自 sqlalchemy 提供的 Model,
_register_func 为注册函数
每个业务的 Model (UserModel, PostsModel, etc.) 都继承 BaseModel 

定义一个模仿的 __go 函数: 
"""


# import 模块时 给 to_dict 函数重新定义
# 首先定义生成函数:
def _generate_to_dict_func(column_name_sets: set):
    """
    :param: column_name_sets:
    :return:
    """

    def to_dict(self):
        return dict((column_name, getattr(self, column_name, None)) for column_name in column_name_sets)

    return to_dict


def _register_func(cls):
    column_name_list = [value[0] for value in cls()._sa_instance_state.attrs.items()]
    column_name_sets = set(column_name_list)

    #  函数被赋予新的指定的行为
    cls.to_dict = _generate_get_to_dict_func(column_name_sets)
    # 优点是每个 Model 都有自己的  column_name_sets
    # _generate_get_to_dict_func 是一个生成闭包函数的函数,
    # 将 column_name_sets 状态保存在生成的闭包函数中.


def __go(lcls):
    """
    inspired from sqlalchemy.__init__.py


    :param lcls:
    :return:
    """
    global __all__

    import inspect as _inspect

    lcls_items = lcls.items()
    class_name_list = []
    model_class_sets = set()

    for name, obj in lcls_items:
        if not (name.startswith('_') or _inspect.ismodule(obj)):
            class_name_list.append(name)
            # 这里模仿了 httpagentparser 识别派生类
            if BaseModel in getattr(obj, '__mro__', []) \
                    and obj is not BaseModel:
                model_class_sets.add(obj)

    __all__ = sorted(class_name_list)

    # 为每个 BaseModel 的派生类注册函数
    for cls in model_class_sets:
        _register_func(cls)


# locals 域中即 __init__.py 里的存储 python 数据对象,
# 包括 import 进来的 BaseModel 和 UserModel等
__go(locals())

# -*- coding: utf-8 -*-
#
#  @file     models/base.py
#  @author   kaka_ace <xiang.ace@gmail.com>
#  @date
#  @brief
#

from sqlalchemy.ext.declarative import (
    declarative_base,
    DeclarativeMeta,
)

from sqlalchemy.orm import aliased


# 元类
class ModelMeta(DeclarativeMeta):
    def __new__(cls, name, bases, d):
        return DeclarativeMeta.__new__(cls, name, bases, d)

    def __init__(self, name, bases, d):
        DeclarativeMeta.__init__(self, name, bases, d)


#
_Base = declarative_base(metaclass=ModelMeta)


class BaseModel(_Base):
    __abstract__ = True

    # 基类的 _column_name_sets  是为实现的类型
    _column_name_sets = NotImplemented

    def to_dict(self):
        """
        """
        return dict(
            (column_name, getattr(self, column_name, None)) \
            for column_name in self._column_name_sets
        )

    @classmethod
    def get_column_name_sets(cls):
        """
        获取 column 的定义的名称(不一定和数据库字段一样)
        """
        return cls._column_name_sets

    __str__ = lambda self: str(self.to_dict())
    __repr__ = lambda self: repr(self.to_dict())


def modelmeta__new__(cls, name, bases, namespace, **kwds):
    column_name_sets = set()
    for k, v in namespace.items():
        if getattr(v, '__class__', None) is None:
            continue
        if v.__class__.__name__ == 'Column':
            column_name_sets.add(k)

    # obj = type.__new__(cls, name, bases, dict(namespace))
    obj = DeclarativeMeta.__new__(cls, name, bases, dict(namespace))
    # update set
    obj._column_name_sets = column_name_sets
    return obj


# modify BaseModel' metatype ModelMeta' __new__ definition
setattr(ModelMeta, '__new__', modelmeta__new__)

# -*- coding: utf-8 -*-
#
# @file     models/base.py
# @author   kaka_ace <xiang.ace@gmail.com>
# @date     Jun 13 2015
# @breif
#


import flask_sqlalchemy
from flask_sqlalchemy import (
    SQLAlchemy,
    _BoundDeclarativeMeta,
)


class ModelMeta(_BoundDeclarativeMeta):
    pass


# 此处类似于 gevent monkey patch 将 socket hook 的方式,
# 这里对 _BoundDeclarativeMeta 运行时被重新赋值.
flask_sqlalchemy._BoundDeclarativeMeta = ModelMeta

db = SQLAlchemy()


class BaseModel(db.Model):
    __abstract__ = True
    _column_name_sets = NotImplemented

    def to_dict(self):
        """
        """
        return dict(
            (column_name, getattr(self, column_name, None)) \
            for column_name in self._column_name_sets
        )

    @classmethod
    def get_column_name_sets(cls):
        """
        """
        return cls.__column_name_sets

    __str__ = lambda self: str(self.to_dict())
    __repr__ = lambda self: repr(self.to_dict())


def modelmeta__new__(cls, name, bases, namespace, **kwds):
    column_name_sets = set()
    for k, v in namespace.items():
        if getattr(v, '__class__', None) is None:
            continue
        if v.__class__.__name__ == 'Column':
            column_name_sets.add(k)

    obj = type.__new__(cls, name, bases, dict(namespace))
    obj._column_name_sets = column_name_sets
    return obj


# modify BaseModel' metatype ModelMeta' __new__ definition
setattr(ModelMeta, '__new__', modelmeta__new__)  
