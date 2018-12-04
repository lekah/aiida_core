# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida_core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""Generic backend related objects"""
from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import abc
import typing
import six

__all__ = ('Backend', 'BackendEntity', 'BackendCollection', 'EntityType')

EntityType = typing.TypeVar('EntityType')  # pylint: disable=invalid-name


@six.add_metaclass(abc.ABCMeta)
class Backend(object):
    """The public interface that defines a backend factory that creates backend specific concrete objects."""

    @abc.abstractproperty
    def authinfos(self):
        """
        Return the collection of authorisation information objects

        :return: the authinfo collection
        :rtype: :class:`aiida.orm.implementation.BackendAuthInfoCollection`
        """

    @abc.abstractproperty
    def comments(self):
        """
        Return the collection of comment objects

        :return: the comment collection
        :rtype: :class:`aiida.orm.implementation.BackendCommentCollection`
        """

    @abc.abstractproperty
    def computers(self):
        """
        Return the collection of computer objects

        :return: the computers collection
        :rtype: :class:`aiida.orm.implementation.BackendComputerCollection`
        """

    @abc.abstractproperty
    def groups(self):
        """
        Return the collection of groups

        :return: the groups collection
        :rtype: :class:`aiida.orm.implementation.BackendGroupCollection`
        """

    @abc.abstractproperty
    def logs(self):
        """
        Return the collection of log entries

        :return: the log collection
        :rtype: :class:`aiida.orm.implementation.BackendLogCollection`
        """

    @abc.abstractproperty
    def query_manager(self):
        """
        Return the query manager for the objects stored in the backend

        :return: The query manger
        :rtype: :class:`aiida.backends.general.abstractqueries.AbstractQueryManager`
        """

    @abc.abstractmethod
    def query(self):
        """
        Return an instance of a query builder implementation for this backend

        :return: a new query builder instance
        :rtype: :class:`aiida.orm.implementation.BackendQueryBuilder`
        """

    @abc.abstractproperty
    def users(self):
        """
        Return the collection of users

        :return: the users collection
        :rtype: :class:`aiida.orm.implementation.BackendUserCollection`
        """


@six.add_metaclass(abc.ABCMeta)
class BackendEntity(object):
    """An first-class entity in the backend"""

    def __init__(self, backend):
        self._backend = backend

    @abc.abstractproperty
    def id(self):  # pylint: disable=invalid-name
        """
        Get the id for this entity.  This is unique only amongst entities of this type
        for a particular backend

        :return: the entity id
        """
        pass

    @property
    def backend(self):
        """
        Get the backend this entity belongs to

        :return: the backend instance
        """
        return self._backend

    @abc.abstractmethod
    def store(self):
        """
        Store this object.

        Whether it is possible to call store more than once is delegated to the object itself
        """
        pass

    @abc.abstractmethod
    def is_stored(self):
        """
        Is the object stored?

        :return: True if stored, False otherwise
        :rtype: bool
        """
        pass


class BackendCollection(typing.Generic[EntityType]):
    """Container class that represents a collection of entries of a particular backend entity."""

    ENTITY_CLASS = None  # type: EntityType

    def __init__(self, backend):
        """
        :param backend: the backend this collection belongs to
        :type backend: :class:`aiida.orm.implementation.Backend`
        """
        assert issubclass(self.ENTITY_CLASS, BackendEntity), "Must set the ENTRY_CLASS class variable to an entity type"
        self._backend = backend

    @property
    def backend(self):
        """
        Return the backend.

        :rtype: :class:`aiida.orm.implementation.Backend`
        """
        return self._backend

    def create(self, **kwargs):
        """
        Create new a entry and set the attributes to those specified in the keyword arguments

        :return: the newly created entry of type ENTITY_CLASS
        """
        return self.ENTITY_CLASS(backend=self._backend, **kwargs)  # pylint: disable=not-callable

    def from_dbmodel(self, dbmodel):
        """
        Create an entity from the backend dbmodel

        :param dbmodel: the dbmodel to create the entity from
        :return: the entity instance
        """
        return self.ENTITY_CLASS.from_dbmodel(dbmodel, self.backend)