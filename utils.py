"""
"""
import inspect
import json


class Singleton(type):

    """Singleton class, just subclass this to obtain a singleton instance of an
    object
    """

    def __init__(cls, *args, **kwargs):
        cls._instance = None
        super().__init__(*args, **kwargs)

    def __call__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__call__(*args, **kwargs)
            return cls._instance
        return cls._instance


class SingletonArgs(type):

    """Singleton that keep single instance for single set of arguments."""

    _instances = {}
    _init = {}

    def __init__(cls, name, bases, dct):
        cls._init[cls] = dct.get("__init__", None)

    def __call__(cls, *args, **kwargs):
        init = cls._init[cls]
        if init is not None:
            key = (
                cls,
                frozenset(inspect.getcallargs(init, None, *args, **kwargs).items()),
            )
        else:
            key = cls

        if key not in cls._instances:
            cls._instances[key] = super(SingletonArgs, cls).__call__(*args, **kwargs)
        return cls._instances[key]


class Configuration(dict, metaclass=Singleton):

    """Configuration singleton base class. Should be subclassed to implement
    different configurations by giving a filepath to read from.
    """

    __initialized__ = False
    __conf__ = {}

    def __new__(cls, *args, **kwargs):
        if not cls.__initialized__:
            cls.__initialized__ = True
            if hasattr(cls, "defaults"):
                setattr(cls, "_defaults", getattr(cls, "defaults"))
            else:
                setattr(cls, "_defaults", {})
        return super().__new__(cls)

    def __init__(self, filename=None):
        if filename is not None:
            try:
                with open(filename, "r") as conf:
                    try:
                        config_dic = json.load(conf)
                    except IOError:
                        pass
                    else:
                        self._defaults.update(config_dic)
            except FileNotFoundError:
                pass
        super().__init__(**self._defaults)

    def __repr__(self):
        return "\n".join(("{}: {}".format(k, v) for k, v in self._defaults.items()))


class ConsistencyUnit:
    """
    Consistency unit, keep a resource and some metrics.
    """

    def __init__(self, data, owners, limit=5):
        self.data = data
        self.hits = 0
        self.lives = 0
        self.limit = limit
        self.owners = owners

    def hit(self):
        """
        Add a hit for this resource, if limit is reached,
        then its data can be replicated by the caller.
        """
        self.hits += 1

        if self.hits == self.limit:
            return True
        return False

    def try_own(self, repLimit):
        """
        Returns True if the resource is in the tolerable replication limit.
        """
        return len(self.owners) < repLimit

    def add_owner(self, owner):
        if owner not in self.owners:
            self.owners.append(owner)

    def remove_owner(self, owner):
        if owner in self.owners:
            self.owners.remove(owner)

    def add_live(self):
        self.lives += 1
        self.hits = 0

    def update_data(self, data, lives=0):
        self.data = data
        self.lives = lives

    def copy(self):
        """
        Returns a copy without data.
        """
        conit = ConsistencyUnit(None, self.owners, self.limit)
        conit.hits = self.hits
        conit.lives = self.lives
        return conit

    def is_removable(self):
        return (self.lives and self.hits < self.limit) or self.lives > 1