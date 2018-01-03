# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida_core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################

import re, copy, collections
from abc import abstractmethod, ABCMeta

from aiida.orm.querybuilder import QueryBuilder
from aiida.orm import (
    Node, Group, Computer, User, Code, Calculation, Data,
    CalculationFactory, DataFactory)
from aiida.common.hashing import make_hash
from aiida.common.extendeddicts import Enumerate
from frozendict import frozendict

# Nothing hard-coded, so I define all strings that are used in this module as frozensets or frozendicts:
ENTITIES = Enumerate(('NODE','GROUP','COMPUTER', 'USER'))
SUBENTITIES = Enumerate(('DATA', 'CALCULATION', 'CODE'))
FACTORIES = Enumerate(('DATAFACTORY', 'CALCULATIONFACTORY'))
OPERATORS = Enumerate(('UPDATE', 'ASSIGN', 'REMOVE'))
RELATIONSHIPS = Enumerate(('LINKED','LEFTLINKED', 'RIGHTLINKED', 'LEFTPATH', 'RIGHTPATH'))

OPERATOR_SYMBOLS = frozendict({
    '+=':OPERATORS.UPDATE,
    '=':OPERATORS.ASSIGN,
    '-=':OPERATORS.REMOVE})

RELATIONSHIP_SYMBOLS = frozendict({
    '--':RELATIONSHIPS.LINKED,
    '<-':RELATIONSHIPS.LEFTLINKED,
    '->':RELATIONSHIPS.RIGHTLINKED,
    '<<':RELATIONSHIPS.LEFTPATH,
    '>>':RELATIONSHIPS.RIGHTPATH})

ENTITY_ABBREVIATIONS = frozendict({
    'N':ENTITIES.NODE,
    'G':ENTITIES.GROUP,
    'U':ENTITIES.USER,
    'C':ENTITIES.COMPUTER,})

SUBENTITY_ABBREVIATIONS = frozendict({
    'DA':SUBENTITIES.DATA,
    'CA':SUBENTITIES.CALCULATION,
    'CO':SUBENTITIES.CODE,})

FACTORY_ABBREVATIONS = frozendict({
    'DF':FACTORIES.DATAFACTORY,
    'CF':FACTORIES.CALCULATIONFACTORY})


class EntityWorld(object):
    """
    The world of an entity (Node, Group, subclasses etc).
    It's an abstract class, since only concrte implementations of a world
    can be used.
    """
    __metaclass__ = ABCMeta
    def __init__(self, aiida_cls):
        base_cls = None
        for cls in (Node, Group, Computer, User):
            if issubclass(aiida_cls, cls):
                base_cls = cls
                break
        if base_cls is None:
            raise ValueError("The class has to be a subclass of Node, Group, Computer or User")
        self._base_cls = base_cls
        self._aiida_cls = aiida_cls
    @property
    def aiida_cls(self):
        return self._aiida_cls

    @property
    def aiida_base_cls(self):
        return self._base_cls


class CollectedWorld(EntityWorld):
    """
    The world of collections. Each collection stores specific keys, and the keys reference AiiDA instances.
    The scope of this world is everything that's stored as a key
    """

    def __str__(self):
        return 'Collected world of {}s'.format(str(self._aiida_cls))
class DatabaseWorld(EntityWorld):
    """
    The world of entities in the databse. The scope of this world is everything that's stored in a database.
    """
    def __str__(self):
        return "DatabaseWorld of {}".format(str(self._aiida_cls))

class InvalidRule(Exception):
    pass


SetOfNodes = CollectedWorld(Node)
SetOfGroups = CollectedWorld(Group)
SetOfComputers = CollectedWorld(Computer)
SetOfUsers = CollectedWorld(User)

# All the AiiDA ORM-classes I know about are referenced in this dictionary:
ENTITY_MAP = frozendict({
    ENTITIES.NODE:Node,
    ENTITIES.GROUP:Group,
    ENTITIES.USER:User,
    ENTITIES.COMPUTER:Computer,
    SUBENTITIES.CALCULATION:Calculation,
    SUBENTITIES.DATA:Data,
    SUBENTITIES.CODE:Code,
    FACTORIES.DATAFACTORY:DataFactory,
    FACTORIES.CALCULATIONFACTORY:CalculationFactory,
})

REVERSE_ENTITY_MAP = frozendict({v:k for k,v in ENTITY_MAP.items()})

ENTITY_IDENTIFIERS = frozendict({
    ENTITIES.NODE:frozendict({'id':int, 'uuid':basestring}),
    ENTITIES.GROUP:frozendict({'id':int, 'uuid':basestring,
        'name':basestring, # Technically,  only name+type is unqique.
            # I'm for leaving the name as if it is was a unique identifier, because for the user it is.
        }),
    ENTITIES.USER:frozendict({'id':int, 'email':basestring}),
    ENTITIES.COMPUTER:frozendict({'id':int, 'email':basestring})
})


NODE2NODE = 'node2node'
NODE2GROUP = 'node2group'
NODE2COMPUTER = 'node2computer'
NODE2USER = 'node2user'



def _get_relationship(specifier):
    """
    :param str specifier: A string that specifies the relationship.

    This function goes through different possibilities to define a relationship,
    either by explicit definition of a string:

        _get_relationship(RELATIONSHIPS.LEFTLINKED)

    Another possibility is to use the abbreviations:

        _get_relationship(RELATIONSHIP_SYMBOLS[RELATIONSHIPS.LEFTLINKED])
    """
    # TODO: More specs for specifying link types as -oi-
    if specifier == "":
        return None
    elif specifier in RELATIONSHIPS:
        return specifier
    elif specifier in RELATIONSHIP_SYMBOLS:
        return RELATIONSHIP_SYMBOLS[specifier]
    else:
        raise ValueError("|{}| is not a valid relationship\nValid specifiers are: {}"
        "".format(specifier, list(RELATIONSHIPS)+RELATIONSHIP_SYMBOLS.keys()))


RULE_REGEX = re.compile("""
    ^(?P<operated>[A-Z])
    (?P<operator>[-|+]? =)
    (?P<projected>[A-Za-z])
    (?P<relationship>((<|-|>) [iorc]? (<|-|>))?)
    (?P<related>[A-Za-z]*)
    (?P<qbspec>(\\ s)?)$""", re.X )





class EntitySet(object):
    """
    Instances of this class reference a subset of entities in a databases via a identifier.
    There are also a few operators defined, for simplicity, to do set-additions (unions) and deletions.
    The underlying Python-class is **set**, which means that adding an instance again to an EntitySet
    will not create a duplicate.
    """
    def __init__(self, aiida_cls=None, cls_name=None, identifier='id'):
        """
        :param aiida_cls: A valid AiiDA ORM class, i.e. Node, Group, Computer
        :param str cls_name:
            Only if aiida_cls is not defined, a valid identifier for the class that
            is found in the ENTITIES.
        :param str identifier:
            The identifier in the database to be used.
            Has to be a valid identifier, as given in VALID_ENTITY_IDENTIFIERS
        """

        if aiida_cls and cls_name:
            raise ValueError("Either define aiida_cls or cls_name")
        if aiida_cls:
            try:
                cls_name = REVERSE_ENTITY_MAP[aiida_cls]
            except KeyError:
                raise ValueError("{} is not a valid input for keyword aiida_cls".format(aiida_cls))
        elif cls_name:
            try:
                aiida_cls = ENTITY_MAP[cls_name]
            except KeyError:
                raise ValueError("{} is not a valid input for keyword cls_name".format(cls_name))
        else:
            # Nothing provided, I exit!
            raise ValueError("Provide either an aiida_cls  or cls_name")

        # For now, I also check that the not a subclass was provided.
        # Subclasses (i.e. Data instead of Node) could be a valid input_cls in the future, but
        # right now there's no need. Since the expected behavior is not guaranteed, I raise here
        if cls_name not in ENTITIES:
            raise ValueError("The class can be: {}".format(', '.join(ENTITIES)))

        try:
            identifier_type = ENTITY_IDENTIFIERS[cls_name][identifier]
        except KeyError:
            raise ValueError("The passed identifier ({}) is not valid."
                "Valid identifiers for {} are:\n"
                "{}".format(identifier, cls_name, ENTITY_IDENTIFIERS[cls_name].keys()))

        # Done with checks, saving to attributes:
        self._aiida_cls = aiida_cls
        # The _set is a set to add the keys:
        self._set = set()
        self._identifier = identifier
        self._identifier_type = identifier_type



    def __len__(self):
        return len(self._set)

    def _check_self_and_other(self, other):
        if self.aiida_cls != other.aiida_cls:
            raise Exception("The two instances do not have the same aiida type!")
        if self.identifier != other.identifier:
            raise Exception("The two instances do not have the same identifier!")
        return True

    def __add__(self, other):
        self._check_self_and_other(other)
        new = EntitySet(self.aiida_cls, identifier=self.identifier)
        new._set_key_set_nocheck(self._set.union(other._set))
        return new

    def __iadd__(self, other):
        """
        Adding inplace!
        """
        self._check_self_and_other(other)
        self._set_key_set_nocheck(self._set.union(other._set))
        return self

    def __sub__(self, other):
        self._check_self_and_other(other)
        new = EntitySet(self.aiida_cls, identifier=self.identifier)
        new._set_key_set_nocheck(self._set.difference(other._set))
        return new

    def __isub__(self, other):
        """
        subtracting inplace!
        """
        self._check_self_and_other(other)
        self._set = self._set.difference(other._set)
        return self

    @property
    def identifier(self):
        return self._identifier

    @property
    def aiida_cls(self):
        return self._aiida_cls


    def _check_input_for_set(self, input_for_set):
        """
        """
        if isinstance(input_for_set, self._aiida_cls):
            return getattr(input_for_set, self._identifier)
        elif isinstance(input_for_set, self._identifier_type):
            return input_for_set
        else:
            raise ValueError("{} is not a valid input\n"
                "You can either pass an AiiDA instance or a key to an instance that"
                "matches the identifier you defined ({})".format(input_for_set, self._identifier_type))

    def set_entities(self, *args):
        new_keys = set()
        for a in args:
            new_keys.add(self._check_input_for_set(a))
        self._set = new_keys

    def add_entities(self, *args):
        new_keys = set()
        for a in args:
            new_keys.add(self._check_input_for_set(a))
        self._set = self._set.union(new_keys)

    def get_keys(self):
        return self._set

    def _set_key_set_nocheck(self, _set):
        """
        Use with care! If you know that the new set is valid, call this function!
        """
        self._set = _set

    def copy(self):
        new = EntitySet(aiida_cls=self.aiida_cls, identifier=self.identifier)
        new._set_key_set_nocheck(self._set.copy())
        return new

class EdgeSet(object):
    def __init__(self,):
        self._links = dict()

    def add_links(self, **kwargs):
        # TODO: check lengths, types, etc
        self._add_links_no_check(kwargs)
    @property
    def links(self):
        return self._links

    def items(self):
        return self._links.items()
    def values(self):
        return self._links.values()
    def keys(self):
        return self._links.keys()

    def _add_link_dict_no_check(self, new_links):
        #~ for k, v in kwargs:
        if not isinstance(new_links, dict):
            raise ValueError("new_links has to be a dictionary")
        self._links.update(new_links)

    def __add__(self, other):
        new = self.__class__()
        new._add_link_dict_no_check(copy.copy(self.links))
        new._add_link_dict_no_check(copy.copy(other.links))
        return new

    def __iadd__(self, other):
        self._add_link_dict_no_check(copy.copy(other.links))
        return self

    def __sub__(self, other):
        raise NotImplementedError

    def __isub__(self, other):
        raise NotImplementedError


class Rule(object):
    __metaclass__ = ABCMeta
    @abstractmethod
    def apply(self, collection, stash):
        """
        Abstract method that every rule needs to implement.
        A rule can manipulate an instance of SubGraph
        or of the stash.
        :param collection: An SubGraph instance
        :param stash:
        """
        pass


class StashCommit(Rule):
    """
    An subclass of Rule, that writes the collection to the stash and returns a copy
    """
    only_update = True
    def __str__(self):
        return '['
    def apply(self, collection, stash):
        stash.append(collection)
        return collection.copy()

class StashPop(Rule):
    only_update = True
    def __str__(self):
        return ']'
    def apply(self, collection, stash):
        return stash.pop(-1)



class Operation(Rule):
    @classmethod
    def get_from_string(cls, string):
        """

        """
        def _get_world_from_string(specifier):
            upspec = specifier.upper()
            if upspec == "": # If nothing was defined:
                return None
            if upspec in ENTITY_ABBREVIATIONS.keys():
                aiida_cls = ENTITY_MAP[ENTITY_ABBREVIATIONS[upspec]]
            elif upspec in SUBENTITY_ABBREVIATIONS.keys():
                aiida_cls = ENTITY_MAP[SUBENTITY_ABBREVIATIONS[upspec]]
            elif upspec in ENTITY_MAP:
                aiida_cls = ENTITY_MAP[upspec]
            else:
                raise ValueError("<{}>".format(specifier))
            # Now, if something was provided with lowercase symbols:
            if specifier.islower():
                return DatabaseWorld(aiida_cls)
            else:
                return CollectedWorld(aiida_cls)

        match = RULE_REGEX.search(string)
        if match is None:
            raise InvalidRule("{} is not a valid rule string".format(string))
        operated = _get_world_from_string(match.group('operated'))
        if not isinstance(operated, CollectedWorld):
            raise ValueError("The operated world has to be a CollectedWorld\n"
                "Hint: Have you used uppercase letters for the operated?")
        operator_spec = match.group('operator')
        try:
            operator = OPERATOR_SYMBOLS[operator_spec]
        except KeyError:
            raise ValueError("{} is not a valid operator. Valid operators are:{}".format(
                operator_spec, ', '.join(OPERATOR_SYMBOLS.keys())))
        relationship_spec = match.group('relationship')
        related_spec = match.group('related')
        if relationship_spec == '':
            relationship = None
            if related_spec:
                raise ValueError("You have defined a related entity without a relationship")
            related = None
        else:
            # TODO:  More specs for specifying link types as -oi-
            relationship = RELATIONSHIP_SYMBOLS[relationship_spec]
            related=_get_world_from_string(related_spec)

        return cls(
            operated=operated, operator=operator,
            projected=_get_world_from_string(match.group('projected')),
            relationship=relationship, related=related,
        )

    def __init__(self, operated, projected, operator,
            related=None, relationship=None, link_tracking=True, string=None,
            projection_filters=None, relationship_filters=None, qb_kwargs=None):

        if not isinstance(operated, CollectedWorld):
            raise ValueError("The operated  has to be a instance of CollectedWorld")
        if not isinstance(projected, EntityWorld):
            raise ValueError("The operated  has to be a instance of EntityWorld")
        if relationship is not None:
            if not isinstance(related, EntityWorld):
                raise ValueError("The related has to be a instance of EntityWorld")
        if related is not None and relationship is None:
            raise ValueError("You have defined a related without the relationship")

        if operator not in OPERATORS:
            raise ValueError("operator {} is not a valid operator ({})".format(operator, ', '.join(OPERATORS)))

        if relationship:
            if relationship not in RELATIONSHIPS:
                raise ValueError("relationship {} is not a valid relationship ({})".format(
                    relationship, ', '.join(RELATIONSHIPS)))

        self._set_entity = operated
        self._projecting_entity = projected
        self._relationship_entity = related
        self._relationship = relationship
        self._operator = operator
        self.set_link_tracking(link_tracking)

        self.set_projection_filters(projection_filters)
        self.set_relationship_filters(relationship_filters)
        self.set_qb_kwargs(qb_kwargs)

    def __str__(self):
        return ' '.join((str(self._set_entity), self._operator, str(self._projecting_entity),
                self._relationship, str(self._relationship_entity)))

    @property
    def only_update(self):
        """
        :returns: A boolean, whether the Operation is an UpdateRule. This allows for tricks in the RuleSequence!
        """
        return self._operator == OPERATORS.UPDATE

    def set_link_tracking(self, link_tracking):
        self._link_tracking = link_tracking

    def set_qb_kwargs(self, kwargs):
        assert kwargs is None or isinstance(kwargs, dict), "filters have to be a valid dictionary"
        self._qb_kwargs = kwargs or {}
    def set_relationship_filters(self, filters):
        assert filters is None or isinstance(filters, dict), "filters have to be a valid dictionary"
        self._relationship_filters = filters
    def set_projection_filters(self, filters):
        assert filters is None or isinstance(filters, dict), "filters have to be a valid dictionary"
        self._projection_filters = filters
    def apply(self, entities_collection, stash=None):
        """
        N = N <- n
        """
        def get_entity_n_filters(entity, entities_collection, additional_filters):
            """
            TODO Docstring, and move out of here!
            """
            if isinstance(entity, CollectedWorld):
                aiida_cls = entity.aiida_cls
                filters={entities_collection[aiida_cls].identifier:{'in':entities_collection[aiida_cls].get_keys()}}
            elif isinstance(entity, DatabaseWorld):
                aiida_cls = entity.aiida_cls
                filters = {}
            else:
                raise ValueError("Unknown EntityWorld {}".format(entity))
            if additional_filters is None:
                pass
            elif isinstance(additional_filters, dict):
                filters.update(additional_filters)
            else:
                raise ValueError("additional filters has to be None or dict")

            return aiida_cls, filters, [entities_collection[aiida_cls].identifier]

        def get_relation_projections(entities_collection, projected, related, relationship):
            # TODO:  some checks
            #~ print relationship, projected, related
            if issubclass(projected, Node):
                if issubclass(related, Node):
                    if relationship in (RELATIONSHIPS.LEFTPATH, RELATIONSHIPS.RIGHTPATH) :
                        edge_projections = ('depth',)
                    elif relationship in (RELATIONSHIPS.LINKED, RELATIONSHIPS.LEFTLINKED, RELATIONSHIPS.RIGHTLINKED):
                        edge_projections = ('label', 'type', 'id')
                    else:
                        raise RuntimeError("Untreated relationship {}".format(relationship))
                elif issubclass(related, Group):
                    edge_projections = None
                elif issubclass(related, (Computer, User)):
                    edge_projections = None
                else:
                    raise RuntimeError("")
            elif issubclass(projected, Group):
                if issubclass(related, Node):
                    edge_projections = None
                else:
                    raise RuntimeError("")
            elif issubclass(projected, (Computer, User)):
                if issubclass(related, Node):
                    edge_projections = None
                else:
                    raise RuntimeError("")
            else:
                raise RuntimeError("")
            return entities_collection[related].identifier, edge_projections


        # Ok, here I need to make some strategic decisions
        # 1) Am I tracking links?
        tracking = self._link_tracking, isinstance(self._relationship_entity, CollectedWorld)
        #~ print 0, tracking,  self._relationship
        # 2) Can I actually track links or is this futile here?
        # 2a) Am I querying a relationship
        if tracking:
            if not self._relationship:
                tracking = False
            # 2b Am I adding the nodes that I'm querying in the relationship to the set or not?
            # That's only the case if projecting nodes are in the db in the relationship nodes in my subset
            elif not isinstance(self._relationship_entity, CollectedWorld):
                tracking = False
            # It seems ok to track things even if the nodes are already on the set. I.e. to get more
            # RELATIONSHIPS!
            #~ elif isinstance(self._projecting_entity, CollectedWorld):
                #~ tracking = False
            elif self._operator != OPERATORS.UPDATE:
                tracking = False
            elif self._relationship not in (RELATIONSHIPS.LINKED, RELATIONSHIPS.LEFTLINKED, RELATIONSHIPS.RIGHTLINKED):
                # I can't deal with
                tracking = False
            else:
                tracking = True

        operated, set_filters, _ = get_entity_n_filters(self._set_entity, entities_collection, {})
        qb_left = QueryBuilder().append(operated, filters=set_filters)


        # qb_list_right is a list of QueryBuilder instances
        # it has to be a list because some RELATIONSHIPS can only be made with
        # several querybuilder instances
        # i.e. N--n (node connected to other node regardless of direction!
        qb_right = [QueryBuilder(**self._qb_kwargs)]
        proj_entity, proj_filters, proj_project = get_entity_n_filters(self._projecting_entity, entities_collection, self._projection_filters)
        qb_right[0].append(proj_entity, filters=proj_filters, project=proj_project, tag='p')
        #~ print
        #~ print 1,proj_entity
        #~ print 2,proj_filters
        #~ print 3,proj_project
        if self._relationship:
            if not self._relationship_entity:
                raise Exception() # should be before, no?

            rlshp_entity, rlshp_filters, rlshp_project = get_entity_n_filters(
                    self._relationship_entity, entities_collection, self._relationship_filters)
            #~ print 4,rlshp_entity
            #~ print 5,rlshp_filters
            #~ print 6,rlshp_project
            if tracking:
                rlshp_project, edge_project = get_relation_projections(
                        entities_collection, proj_entity, rlshp_entity, self._relationship)
                #~ print edge_project
                orders = []
                edge_identifiers = []

            else:
                rlshp_project = None
                edge_project = None
            if issubclass(proj_entity, Node):
                if issubclass(rlshp_entity, Node):
                    edge_name = NODE2NODE
                    if self._relationship == RELATIONSHIPS.LEFTLINKED:
                        qb_right[0].append(rlshp_entity, input_of='p', filters=rlshp_filters,
                                project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0,2,3))
                            edge_identifiers.append(4)
                    elif self._relationship == RELATIONSHIPS.RIGHTLINKED:
                        qb_right[0].append(rlshp_entity, output_of='p', filters=rlshp_filters,
                                project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1,2,3))
                            edge_identifiers.append(4)
                    elif self._relationship == RELATIONSHIPS.RIGHTPATH:
                        qb_right[0].append(rlshp_entity, descendant_of='p', filters=rlshp_filters,
                                project=rlshp_project, edge_project=edge_project)
                    elif self._relationship == RELATIONSHIPS.LEFTPATH:
                        qb_right[0].append(rlshp_entity, ancestor_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                    elif self._relationship == RELATIONSHIPS.LINKED:

                        qb_right.append(qb_right[0].copy())
                        qb_right[0].append(rlshp_entity, output_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        qb_right[1].append(rlshp_entity, input_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1,2,3))
                            orders.append((1,0,2,3))
                            # Repetition below is NOT a bug!
                            edge_identifiers.append(4)
                            edge_identifiers.append(4)
                    else:
                        raise RuntimeError("")
                elif issubclass(rlshp_entity, Group):
                    edge_name = NODE2GROUP
                    if self._relationship == RELATIONSHIPS.LINKED:
                        qb_right[0].append(rlshp_entity, group_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1))
                            edge_identifiers.append(None)
                    else:
                        raise RuntimeError("")

                elif issubclass(rlshp_entity, User):
                    edge_name = NODE2USER
                    if self._relationship == RELATIONSHIPS.LINKED:
                        qb_right[0].append(rlshp_entity, creator_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1))
                            edge_identifiers.append(None)
                    else:
                        raise RuntimeError("")
                elif issubclass(rlshp_entity, Computer):
                    edge_name = NODE2COMPUTER
                    if self._relationship == RELATIONSHIPS.LINKED:
                        qb_right[0].append(rlshp_entity, computer_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1))
                            edge_identifiers.append(None)
                    else:
                        raise RuntimeError("")
                else:
                    raise NotImplementedError
            elif issubclass(proj_entity, Group):
                edge_name = NODE2GROUP
                if issubclass(rlshp_entity, Node):
                    #~ print 'HERE'
                    if self._relationship == RELATIONSHIPS.LINKED:
                        qb_right[0].append(rlshp_entity,
                                member_of='p', filters=rlshp_filters,
                                project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0))
                            edge_identifiers.append(None)
                    else:
                        raise RuntimeError("")

            elif issubclass(proj_entity, User):
                edge_name = NODE2USER
                if issubclass(rlshp_entity, Node):
                    if self._relationship == RELATIONSHIPS.LINKED:
                        qb_right[0].append(rlshp_entity, created_by='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0))
                            edge_identifiers.append(None)
                    else:
                        raise RuntimeError("")

            elif issubclass(proj_entity, Computer):
                edge_name = NODE2COMPUTER
                if issubclass(rlshp_entity, Node):
                    if self._relationship == RELATIONSHIPS.LINKED:
                        qb_right[0].append(rlshp_entity, has_computer='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0))
                            edge_identifiers.append(None)
                    else:
                        raise RuntimeError("")
            else:
                raise RuntimeError("")

        if tracking:
            res = [qb.all() for qb in qb_right]
            qb_right_result = [_[0] for  subres in res for _ in subres]
        else:
            qb_right_result = set([_ for qb in qb_right for _, in qb.iterall()])
        #~ print 8, qb_right_result, self._operator
        if self._operator == OPERATORS.ASSIGN:
            #~ print entities_collection[operated].set
            #~ print qb_right.all()
            entities_collection[operated]._set_key_set_nocheck(qb_right_result)

        elif self._operator == OPERATORS.UPDATE:
            entities_collection[operated]._set_key_set_nocheck(
                entities_collection[operated]._set.union(qb_right_result))

            if tracking:
                for o, subres,identifier in zip(orders, res, edge_identifiers):
                    for row in subres:
                        if identifier:
                            edge_id = row[identifier]
                        else:
                            edge_id = make_hash(row)
                        entities_collection[edge_name]._add_link_dict_no_check({edge_id:tuple(row[i] for i in o)})

        elif self._operator == OPERATORS.REMOVE:
            entities_collection[operated]._set_key_set_nocheck(
                    entities_collection[operated]._set.difference(qb_right_result))
        else:
            raise Exception("Unknown operator <{}>".format(self._operator))
        return entities_collection


class RuleSequence(Rule):
    """
    A RuleSequence is - as the name suggests - a sequence of rules.
    *   I want to get all nodes that belong to all groups that the nodes in my set belong to: G=g--N N=n--G
    *   I want to get the inputs of the inputs: N=n->N N=n->N  or (N=n->N)2
    *   I want to get all ancestors!
    """

    @classmethod
    def get_from_string_old(cls, rule_spec):
        """
        Get a RuleSequence from the specification
        """


        rules = []
        escaping = False
        commits_to_stash_counter = 0
        news = ""
        s = iter(rule_spec)
        try:
            while True:
                c = s.next()
                #~ print c
                if escaping:
                    news += c
                    escaping = False
                elif c == '\\':
                    escaping = True
                    news += c
                elif c in ' [](':
                    if news:
                        rules.append(Operation.get_from_string(news))
                        news = ""
                    if c == ' ':
                        pass
                    elif c == '[':
                        rules.append(StashCommit())
                        commits_to_stash_counter += 1
                    elif c == ']':
                        if commits_to_stash_counter == 0:
                            raise InvalidRule("You're popping more times from stash than you commit")
                        rules.append(StashPop())
                        commits_to_stash_counter -= 1
                    elif c == '(':
                        open_brackets = 1
                        #~ print "searching for final bracket"
                        try:
                            while True:
                                c = s.next()
                                #~ print ' ', c
                                if escaping:
                                    escaping = False
                                    news+=c
                                elif c == '\\':
                                    escaping = True
                                    news += c
                                elif c == '(':
                                    open_brackets += 1
                                elif c == ')':
                                    open_brackets -= 1
                                    if open_brackets == 0:
                                        #~ print "The final bracket"
                                        break
                                else:
                                    news += c
                        except StopIteration:
                            raise InvalidRule("Brackets not closed")
                        rs = RuleSequence.get_from_string(news)
                        # What is the value behind:
                        numspec = ''
                        try:
                            while True:
                                c = s.next()
                                if c in '0123456789':
                                    numspec += c
                                elif c == '*':
                                    numspec = '*'
                                    raise StopIteration
                                else:
                                    raise StopIteration
                        except StopIteration:
                            if numspec == '*':
                                niter = -1
                            elif numspec:
                                niter = int(numspec)
                            else:
                                niter = 1
                        #~ print "adding", rs, "with niter={}".format(niter)
                        rs.set_niter(niter)
                        rules.append(rs)
                        news = ""
                else:
                    news += c
        except StopIteration:
            if news:
                rules.append(Operation.get_from_string(news))
        return RuleSequence(*rules)

    @classmethod
    def get_from_string(cls, rule_spec, recursion_depth=0):
        """
        Get a RuleSequence from the specification
        """
        rules = [] # Saving the rules here
        escaping = False # In case special characters (][ etc) are kept, escape them with \
        commits_to_stash_counter = 0 # I count how many times I commit to stash, and how many times I pop
        operation_spec = "" 
        iterable_rule_spec = iter(rule_spec)
        try:
            while True:
                new_character = iterable_rule_spec.next()
                if escaping: # If escaping, just add the character to the spec
                    operation_spec += new_character
                    escaping = False # Escaping only one character, so escaping is now set to False
                elif new_character == '\\': # This is the escaping character: \
                    operation_spec += new_character # Still being added, the final cleaning is done in Operation.get_from_string!
                    escaping = True 
                elif new_character in ' []()':
                    if operation_spec:
                        # These special charactes mark the end of an operation specfication
                        rules.append(Operation.get_from_string(operation_spec))
                        operation_spec = "" # Starting new spec
                    if new_character == ' ':
                        pass # Ignoring additional spaces
                    elif new_character == '[':
                        rules.append(StashCommit())
                        commits_to_stash_counter += 1
                    elif new_character == ']':
                        if commits_to_stash_counter == 0:
                            raise InvalidRule("You're popping more times from stash than you commit")
                        rules.append(StashPop())
                        commits_to_stash_counter -= 1
                    elif new_character == '(':
                        # If a bracket is opened, I start a new rule_sequence
                        new_rule_sequence = RuleSequence.get_from_string(
                                iterable_rule_spec, recursion_depth=recursion_depth+1)
                        # It will return when this specific bracket is closed.
                        # Right after, there can be (not necessarili a specification for the
                        # number of iterations
                        try:
                            numspec = ''
                            while True:
                                new_character = iterable_rule_spec.next()
                                if new_character in '0123456789':
                                    numspec += new_character
                                elif new_character == '*':
                                    numspec = '*'
                                    raise StopIteration
                                else:
                                    raise StopIteration
                        except StopIteration:
                            if numspec == '*':
                                niter = -1
                            elif numspec:
                                niter = int(numspec)
                            else:
                                niter = 1
                        #~ print "adding", rs, "with niter={}".format(niter)
                        new_rule_sequence.set_niter(niter)
                        rules.append(new_rule_sequence)
                        operation_spec = ""
                    elif new_character == ')':
                        if recursion_depth == 0:
                            raise InvalidRule("Closing a bracket that was not opened")
                        else:
                            break
                else:
                    operation_spec += new_character
        except StopIteration:
            if operation_spec:
                rules.append(Operation.get_from_string(operation_spec))
        if not len(rules):
            raise InvalidRule("Empty rule")
        return RuleSequence(*rules)

    def __str__(self):
        return '({}){}'.format( ' '.join([str(r) for r in self._rules]), self._niter)

    def __init__(self, *rules, **kwargs):
        """
        :param *rules: Instances of Operation, RuleSequence, StashPop and StashCommit that will be executed in that order
        :param
        """
        for r in rules:
            if not isinstance(r, Rule):
                raise ValueError("{} {} is not a valid input".format(type(r), r))

        self.set_niter(kwargs.pop('niter', 1))
        self._track_links = bool(kwargs.pop('track_links', False))

        if kwargs:
            raise Exception("Unrecognized keywords {}".format(kwargs.keys()))
        self._rules = rules
        self._last_niter = None
        self._stash = []


    @property
    def only_update(self):
        """
        :returns: A boolean, whether the Operation is an UpdateRule. This allows for tricks in the RuleSequence!
        """
        return all([r.only_update for r in self._rules])

    def apply(self, main_collection, stash=None):
        # I ignore the stash, it's also an attribute of self. For compliance with Rule.apply!
        if self.only_update:
            return self._apply_rules_with_tricks(main_collection)
        else:
            return self._apply_rules_no_tricks(main_collection)
    def _apply_rules_with_tricks(self, collection):
        """
        This is a rule sequence that does only updates on a given collection.
        I can therefore apply some tricks
        """
        iterations = 0

        # The visited_collection is all the nodes I visited during my exploration!
        # Also what I will return!
        visited_collection = collection
        # I also have an operation_collection, the main thing being updated
        operational_collection = collection.copy()
        # dealt_with_collection is the collection of everything that has seen ALL the rules applied
        # to itself, i.e. that was present in the beginning of a loop.
        # NOTE: not the same as visited if we have stash commits/push
        dealt_with_collection = collection.copy()
        while True:
            if iterations == self._niter:
                break
            if not operational_collection:
                break
            #~ operational_collection = new_collection.copy()
            for item in self._rules:
                operational_collection = item.apply(operational_collection, self._stash)
                # I update here.
                # TODO: Tricks with checking whether Stashe etc to avoid number of updates?
                visited_collection += operational_collection
            # Now I update the visited collection which is the collection I keep track of:
            # So, what here is actually new?
            operational_collection -= dealt_with_collection
            dealt_with_collection = dealt_with_collection + operational_collection
            iterations += 1
        self._last_niter = iterations
        return visited_collection


    def _apply_rules_no_tricks(self, collection):
        iterations = 0
        operational_collection = collection
        while True:
            if iterations == self._niter:
                break
            if not operational_collection:
                break
            #~ operational_collection = new_collection.copy()
            for item in self._rules:
                operational_collection = item.apply(operational_collection, self._stash)
            iterations += 1
        self._last_niter = iterations
        return operational_collection

    def get_last_niter(self):
        return self._last_niter

    def set_niter(self, niter):
        if not isinstance(niter, int):
            raise ValueError("niter has to be an integer")
        self._niter = niter


class SubGraph(object):
    """
    Basically an AiiDA subgraph
    """
    def __init__(self, node_identifier='id', group_identifier='id',
            user_identifier='id', computer_identifier='id'):

        self.nodes = EntitySet(Node, identifier=node_identifier)
        self.groups = EntitySet(Group, identifier=group_identifier)
        self.computers = EntitySet(Computer, identifier=computer_identifier)
        self.users = EntitySet(User, identifier=user_identifier)
        # The following are for the edges,
        # for now, dictionaries with the link_id being the key!
        # the values can be defined as they want, as long as it's consistent...
        # for now I chose tuples from all the stuff I can get
        # entry_in, entry_out, ...

        self.nodes_nodes = EdgeSet()
        self.nodes_groups = EdgeSet()
        self.nodes_computers = EdgeSet()
        self.nodes_users = EdgeSet()

    def __getitem__(self, key):
        if key in (Node, ENTITIES.NODE):
            return self.nodes
        elif key in (Group, ENTITIES.GROUP):
            return self.groups
        elif key in (Computer, ENTITIES.COMPUTER):
            return self.computers
        elif key in (User, ENTITIES.USER):
            return self.users
        elif key == NODE2NODE:
            return self.nodes_nodes
        elif key == NODE2GROUP:
            return self.nodes_groups
        elif key == NODE2COMPUTER:
            return self.nodes_computers
        elif key == NODE2USER:
            return self.nodes_users
        else:
            raise KeyError(key)

    def __setitem__(self, key, val):
        if key == NODE2NODE:
            self.nodes_nodes = val
        elif key == NODE2GROUP:
            self.nodes_groups = val
        elif key == NODE2COMPUTER:
            self.nodes_computers = val
        elif key == NODE2USER:
            self.nodes_users = val
        else:
            raise KeyError(key)

    def __len__(self):
        return sum([len(_) for _ in self.values()])

    def __add__(self, other):
        new = SubGraph()
        new.nodes = self.nodes + other.nodes
        new.groups = self.groups + other.groups
        new.computers = self.computers + other.computers
        new.users = self.users + other.users
        for k in (NODE2COMPUTER, NODE2NODE, NODE2GROUP, NODE2USER):
            new[k] = self[k] + other[k]

        return new


    def __iadd__(self, other):
        self.nodes += other.nodes
        self.groups += other.groups
        self.computers += other.computers
        self.users += other.users
        for k in (NODE2COMPUTER, NODE2NODE, NODE2GROUP, NODE2USER):
            self[k] = self[k] + other[k]
        return self


    def __sub__(self, other):
        new = SubGraph()
        new.nodes = self.nodes - other.nodes
        new.groups = self.groups - other.groups
        new.computers = self.computers - other.computers
        new.users = self.users - other.users
        #~ new.nodes_nodes = self.nodes_nodes - other.nodes_nodes

        return new

    def __isub__(self, other):
        self.nodes -= other.nodes
        self.groups -= other.groups
        self.computers -=  other.computers
        self.users -= other.users
        #~ self.nodes_nodes -= other.nodes_nodes

        return self

    def values(self):
        return (self.nodes, self.groups, self.computers, self.users)

    def copy(self):
        other = self.__class__()
        other.nodes = self.nodes.copy()
        other.groups = self.groups.copy()
        other.computers = self.computers.copy()
        other.users = self.users.copy()

        return other

class GraphExplorer(object):
    def __init__(self, recipe,
            node_pks=None, node_uuids=None, group_pks=None, group_names=None, group_uuids=None,
            node_identifier='id', group_identifier='id'):
        # starting with the nodes:
        node_ors = []
        if node_pks:
            node_ors.append({'id':{'in':node_pks}})
        if node_uuids:
            node_ors.append({'uuid':{'in':node_uuids}})
        if node_ors:
            qb = QueryBuilder()
            qb.append(Node, filters={'or':node_ors}, project=node_identifier)
            node_identifiers = set([_ for _, in  qb.distinct().iterall()])
        else:
            node_identifiers = set()


        group_ors = []
        if group_pks:
            group_ors.append({'id':{'in':group_pks}})
        if group_uuids:
            group_ors.append({'uuid':{'in':group_uuids}})
        if group_names:
            group_ors.append({'name':{'in':group_names}})
        if group_ors:
            qb = QueryBuilder()
            qb.append(Group, filters={'or':group_ors}, project=group_identifier)
            group_identifiers = set([_ for _, in  qb.distinct().iterall()])
        else:
            group_identifiers = set()

        self._entities_collection = SubGraph()
        self._entities_collection.nodes._set_key_set_nocheck(node_identifiers)
        self._entities_collection.groups._set_key_set_nocheck(group_identifiers)

        self._recipe = recipe
    def explore(self):
        RuleSequence.get_from_string(self._recipe).apply(self._entities_collection)
        #~ print self._entities_collection.nodes._set


    def draw(self):
        draw_graph(self._entities_collection,
                format='pdf',
                #~ format='dot',
                filename='temp')

def draw_graph(entities_collection, origin_node=None, format='dot', filename=None):
    import os, tempfile
    from aiida.orm import load_node
    from aiida.orm.calculation import Calculation
    from aiida.orm.calculation.job import JobCalculation
    from aiida.orm.code import Code
    from aiida.orm.node import Node
    from aiida.common.links import LinkType
    from aiida.orm.querybuilder import QueryBuilder
    def draw_node_settings(node, **kwargs):
        """
        Returns a string with all infos needed in a .dot file  to define a node of a graph.
        :param node:
        :param kwargs: Additional key-value pairs to be added to the returned string
        :return: a string
        """
        if isinstance(node, Calculation):
            shape = "shape=polygon,sides=4"
        elif isinstance(node, Code):
            shape = "shape=diamond"
        else:
            shape = "shape=ellipse"
        if kwargs:
            additional_params = ",{}".format(
                ",".join('{}="{}"'.format(k, v) for k, v in kwargs.iteritems()))
        else:
            additional_params = ""
        if node.label:
            label_string = "\n'{}'".format(node.label)
            additional_string = ""
        else:
            label_string = ""
            descr = node.get_desc()
            if descr:
                additional_string = "\n {}".format(node.get_desc())
            else:
                additional_string = ''

        labelstring = 'label="{} ({}){}{}"'.format(
            node.__class__.__name__, node.pk, label_string,
            additional_string)
        return "N{} [{},{}{}];".format(node.pk, shape, labelstring,
                                       additional_params)

    def draw_link_settings(inp_id, out_id, link_label, link_type):
        if link_type in (LinkType.CREATE.value, LinkType.INPUT.value):
            style='solid'  # Solid lines and black colors
            color = "0.0 0.0 0.0" # for CREATE and INPUT (The provenance graph)
        elif link_type == LinkType.RETURN.value:
            style='dotted'  # Dotted  lines of
            color = "0.0 0.0 0.0" # black color for Returns
        elif link_type == LinkType.CALL.value:
            style='bold' # Bold lines and
            color = "0.0 1.0 1.0" # Bright red for calls
        else:
            style='solid'   # Solid and
            color="0.0 0.0 0.5" #grey lines for unspecified links!
        return '    {} -> {} [label="{}", color="{}", style="{}"];'.format("N{}".format(inp_id),  "N{}".format(out_id), link_label, color, style)
    # Writing the graph to a temporary file
    def draw_group_settings(group, connected_nodes_pks):
        return ('subgraph cluster_{} {{\n'
            '{};\n'
            'label="{}";\n'
            'color=blue;\n'
            '}}\n').format(group.pk, ' '.join(['"N{}"'.format(pk) for pk in connected_nodes_pks]), group.name)


    #~ fd, fname = tempfile.mkstemp(suffix='.dot')
    fname = 'test.dot'
    nodes = {}
    links = {}
    groups = {}

    for node, in QueryBuilder().append(Node, filters={
            entities_collection.nodes.identifier:{'in':entities_collection.nodes.get_keys()}}).iterall():
        nodes[node.id] = draw_node_settings(node)
    for link_key, link_vals in entities_collection.nodes_nodes.items():
        links[link_key] = draw_link_settings(*link_vals)

    group_connections = collections.defaultdict(list)
    taken_nodes = set()
    for node_id, group_id in entities_collection.nodes_groups.values():
        if node_id in taken_nodes:
            raise Exception("Unfortunately, graphviz cannot visualize overlapping containers"
                "The node with pk={} seems to belong to several groups".format(node_id))
        taken_nodes.add(node_id)
        group_connections[group_id].append(node_id)
        # Graphviz cannot visualize overlapping containers

    for group, in QueryBuilder().append(Group, filters={
            entities_collection.groups.identifier:{'in':entities_collection.groups.get_keys()}}).iterall():
        if group.pk not in group_connections:
            # This group is not connected to any nodes
            continue
        groups[group.pk] = draw_group_settings(group, group_connections[group.pk])

    if isinstance(origin_node, (int,basestring)):
        origin_node  = load_node(origin_node)

    if origin_node is not None:
        nodes[origin_node.pk] = draw_node_settings(origin_node, style='filled', color='lightblue')
    with open(fname, 'w') as fout:
        fout.write("digraph G {\n")
        #~ print "digraph G {"
        for _, groupspec in groups.iteritems():
            fout.write(groupspec)
        for l_name, l_values in links.iteritems():
            fout.write('    {}\n'.format(l_values))
        for n_name, n_values in nodes.iteritems():
            fout.write("    {}\n".format(n_values))

        fout.write("}\n")

    # Now I am producing the output file
    output_file_name = "{0}.{format}".format(filename or origin_node.pk, format=format)
    exit_status = os.system('dot -T{format} {0} -o {1}'.format(fname, output_file_name, format=format))
    # cleaning up by removing the temporary file
    #~ os.remove(fname)
    return exit_status, output_file_name
