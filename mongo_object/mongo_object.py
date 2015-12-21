#! /usr/bin/env python
# -*- encoding: utf-8 -*-


from pymongo import MongoClient
import bson
from bson import ObjectId
# from bson import SON
from datetime import datetime
from datetime import timedelta
import simplejson


# HOW TO USE IT?
#
# Create a subclass, and define host, db, collection as class variables
#
# class MyAppClass(MongoObject):
#     db_host = 'localhost'
#     db_name = 'my_application_db'
#     collection_name = 'my_app_class'
#
# Then add your own object methods...


DEFAULT_DB_HOST = 'localhost'
DEFAULT_DB_NAME = 'mongo_object'
DEFAULT_COLLECTION_NAME = 'mongo_object'


class SConnections(object):

    _ref = {}

    def __new__(cls, host, *args, **kwargs):
        if host in cls._ref:
            return cls._ref.get(host)
        else:
            instance = super(SConnections, cls).__new__(cls, *args, **kwargs)
            instance.mongo_connection = MongoClient(host)
            cls._ref[host] = instance
            return cls._ref.get(host)

    def __call__(self):
        return self.mongo_connection


class InitError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class IdError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class MongoObject(object):

    db_host = DEFAULT_DB_HOST
    db_name = DEFAULT_DB_NAME
    collection_name = DEFAULT_COLLECTION_NAME

    def __init__(self, object_dict, *args, **kwargs):
        '''Initialize MongoObject from dict
        '''
        object.__init__(self, *args, **kwargs)
        self.db = SConnections(self.db_host)()[self.db_name]
        if not 'creation_date' in object_dict:
            object_dict['creation_date'] = datetime.now()

        self.md = object_dict

    @classmethod
    def from_dict(cls, object_dict, *args, **kwargs):
        '''Creates MongoObject from dict
        returns MongoObject instance
        '''
        instance = super(MongoObject, cls).__new__(cls, *args, **kwargs)
        instance.db = SConnections(cls.db_host)()[cls.db_name]
        if not 'creation_date' in object_dict:
            object_dict['creation_date'] = datetime.now()

        instance.md = object_dict

        return instance

    @classmethod
    def load_from_query_son(cls, query, sort=None, *args, **kwargs):
        '''Loads *one* object in collection_name based on query
        Returns MongoObject instance if found
        Returns None object if not found
        '''
        if '_id' in query:
            query['_id'] = ObjectId(query['_id'])
        instance = super(MongoObject, cls).__new__(cls, *args, **kwargs)
        instance.db = SConnections(cls.db_host)()[cls.db_name]
        mongo_dict = instance.db[cls.collection_name].find_one(
            query,
            sort=sort)
        if mongo_dict is None:
            return None
        else:
            instance.md = mongo_dict
            return instance

    @classmethod
    def load_from_key(cls, key, value, *args, **kwargs):
        '''Loads *one* object in collection_name based on key/value
        Returns MongoObject instance if found
        Returns None object if not found
        '''
        instance = cls.load_from_query_son({key: value}, *args, **kwargs)
        return instance

    @classmethod
    def load_from_objectid(cls, objectid, *args, **kwargs):
        '''Loads object from ObjectId
        Uses load_from_key method
        Returns MongoObject instance if found
        Returns None object if not found
        '''
        try:
            instance = cls.load_from_key('_id', ObjectId(objectid), *args, **kwargs)
            return instance
        except bson.errors.InvalidId:
            return None

    @classmethod
    def find_from_query_son(cls, query_son, sort=None, limit=None, skip=None, *args, **kwargs):
        '''Returns an iterator (result of pymongo find)
        Warning: On this method, collection_name is always used when supplied
        '''
        if '_id' in query_son:
            query_son['_id'] = ObjectId(query_son['_id'])
        res = SConnections(cls.db_host)()[cls.db_name][cls.collection_name].find(query_son)
        if sort:
            res = res.sort(sort)
        if limit:
            res = res.limit(limit)
        if skip:
            res = res.skip(skip)
        for result in res:
            yield cls.from_dict(result)

    @classmethod
    def count_from_query_son(cls, query_son, *args, **kwargs):
        if '_id' in query_son:
            query_son['_id'] = ObjectId(query_son['_id'])
        return SConnections(
            cls.db_host
        )()[cls.db_name][cls.collection_name].find(query_son).count()

    @classmethod
    def find_and_modify(cls, query_son, update=None, sort=None):
        result = SConnections(
            cls.db_host
        )()[cls.db_name][cls.collection_name].find_and_modify(
            query=query_son,
            sort=sort,
            update=update
        )
        if result is None:
            return None
        return cls.from_dict(result)

    def save(self):
        '''Saves a MongoObject
        Returns _id
        '''
        dict_to_save = dict(self.md)
        # remove types that cannot be saved (timedelta)...
        dict_to_save = dict([
            (k, dict_to_save[k])
            for k in dict_to_save.keys()
            if not isinstance(dict_to_save[k], timedelta)
        ])

        self.md['_id'] = self.db[self.collection_name].save(dict_to_save)
        return self.md['_id']

    def remove(self):
        '''Remove object from DB
        Returns _id if success
        Returns None if object is not in db
        '''
        _id = self.md.get('_id', None)
        if _id:
            self.db[self.collection_name].remove(
                {'_id': _id}
            )
            return _id
        else:
            return None

    def reload(self):
        '''Reloads __dict__ from database
        Requires _id attribute in MongoObject
        Returns ObjectId
        '''
        _id = self.md.get('_id', None)
        if _id:
            self.db = SConnections(self.db_host)()[self.db_name]
            mongo_dict = self.db[self.collection_name].find_one({'_id': _id})
            self.md = mongo_dict
            return self.md.get('_id')
        else:
            raise IdError('object has no id')

    def update(self, update_dict, upsert=False, k='_id'):
        v = self.md.get(k, None)
        if v is not None:
            find_dict = {}
            find_dict[k] = self.md.get(k)
            res = self.db[self.collection_name].update(
                find_dict,
                update_dict,
                upsert=upsert)
            if u'upserted' in res:
                self.md['_id'] = res.get('upserted')
            if '_id' in self.md:
                self.reload()
        else:
            raise IdError('object has no specified key')

    def get(self, *args, **kwargs):
        return self.md.get(*args, **kwargs)

    @classmethod
    def aggregate(cls, aggregation_list):
        return SConnections(
            cls.db_host
        )()[cls.db_name][cls.collection_name].aggregate(aggregation_list)

    @staticmethod
    def _json_additional_support(o):
        if(isinstance(o, datetime)):
            return o.isoformat() + "z"
        elif(isinstance(o, ObjectId)):
            return str(o)
        else:
            raise TypeError(repr(o) + " is not JSON serializable")

    def jsonable(self):
        return simplejson.loads(self.to_json())

    def to_json(self):
        return simplejson.dumps(self.md, default=MongoObject._json_additional_support)

    def id(self):
        return self.get('_id', None)

    def id_str(self):
        if(self.id()):
            return str(self.id())
        return None
