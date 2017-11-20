import re

from aiida.orm.querybuilder import QueryBuilder
from aiida.orm.node import Node
from aiida.orm.group import Group
from aiida.orm.computer import Computer
from aiida.orm.user import User



BASE_DEFAULT_DICT = {'n':Node, 'u':User, 'g':Group, 'c':Computer}

VALID_RELATIONSHIPS = {'--', '<-', '->', '>>', '<<', None}
VALID_OPERATORS = {'=', '+=', '-=', None}

class SubSetOfDB(object):
    def __init__(self, aiida_type):
        self._aiida_type = aiida_type
    @property
    def aiida_type(self):
        return self._aiida_type


def _get_aiida_entity_from_string(specifier):
    lowspec = specifier.lower()
    if lowspec == "":
        return None
    elif lowspec in BASE_DEFAULT_DICT:
        aiida_type = BASE_DEFAULT_DICT[lowspec]
    elif lowspec == 'b':
        # import and return BandsData!
        pass
    else:
        raise NotImplementedError("<{}>".format(specifier))
    if specifier.islower():
        return aiida_type
    else:
        return SubSetOfDB(aiida_type)

def _get_relationship(specifier):
    # TODO: More specs for specifying link types as -oi-
    if specifier not in VALID_RELATIONSHIPS:
        raise ValueError("|{}| is not a valid relationship\nValid specifiers are: {}".format(specifier, VALID_RELATIONSHIPS))
    return specifier

def _get_operator(specifier):
    # TODO: More specs for specifying link types as -oi-
    if specifier not in VALID_OPERATORS:
        raise ValueError("{} is not a valid operator".format(specifier))
    return specifier


RULE_REGEX = re.compile("""
    ^(?P<set_entity>[A-Z])
    (?P<operator>[-|+]? =)
    (?P<projecting_entity>[A-Za-z])
    (?P<relationship>((<|-|>) [iorc]? (<|-|>))?)
    (?P<relationship_entity>[A-Za-z]*)
    (?P<qbspec>(\\ s)?)$""", re.X )

#~ 
#~ def update_qb_instance(qb, entity, **kwargs):
    #~ if isinstance(entity, SubSetOfDB):
        #~ aiida_type = entity.aiida_type
    #~ else:
        #~ aiida_type = entity
    #~ qb.append(entity

class Rule(object):
    @classmethod
    def get_from_string(cls, string):
        """
        
        """
        match = RULE_REGEX.search(string)
        if match is None:
            raise ValueError("{} is not a valid rule string".format(string))
        return cls(
            set_entity=_get_aiida_entity_from_string(match.group('set_entity')),
            operator=_get_operator(match.group('operator')),
            projecting_entity=_get_aiida_entity_from_string(match.group('projecting_entity')),
            relationship=_get_relationship(match.group('relationship')),
            relationship_entity=_get_aiida_entity_from_string(match.group('relationship_entity')),
        )

        
        
    def __init__(self, set_entity, projecting_entity, operator, relationship_entity=None, relationship=None):
        if not isinstance(set_entity, SubSetOfDB):
            raise ValueError()
        self._set_entity = set_entity
        self._projecting_entity = projecting_entity
        self._relationship_entity = relationship_entity
        self._relationship = relationship
        self._operator = operator
    def apply(self, entities_collection):
        """
        N = N <- n
        """

        def get_entity_n_filters(entity, additional_filters):

            if isinstance(entity, SubSetOfDB):
                aiida_type = entity.aiida_type
                filters={entities_collection[aiida_type].identifier:{'in':entities_collection[aiida_type].set}}
            else:
                aiida_type = entity
                filters = {}
            return aiida_type, filters, [entities_collection[aiida_type].identifier]

            # Todo add filters from spec!
        set_entity, set_filters, _ = get_entity_n_filters(self._set_entity, {})
        qb_left = QueryBuilder().append(set_entity, filters=set_filters)
        
        qb_right = QueryBuilder()
        proj_entity, proj_filters, proj_project = get_entity_n_filters(self._projecting_entity, {})
        qb_right.append(proj_entity, filters=proj_filters, project=proj_project, tag='p')
        if self._relationship:

            if not self._relationship_entity:
                raise Exception() # should be before, no
            rlshp_entity, rlshp_filters, _ = get_entity_n_filters(self._relationship_entity, {})
            if issubclass(proj_entity, Node):
                if issubclass(rlshp_entity, Node):
                    if self._relationship == '<-':
                        qb_right.append(rlshp_entity, input_of='p', filters=rlshp_filters)
                    elif self._relationship == '->':
                        qb_right.append(rlshp_entity, output_of='p', filters=rlshp_filters)
                    else:
                        raise NotImplemented
                if issubclass(rlshp_entity, Group):
                    if self._relationship == '--':
                        qb_right.append(rlshp_entity, group_of='p')
        if self._operator == '=':
            #~ print entities_collection[set_entity].set
            #~ print qb_right.all()
            entities_collection[set_entity].set = set([_ for _, in qb_right.iterall()])

        elif self._operator == '+=':
            entities_collection[set_entity].set =  entities_collection[set_entity] + set([_ for _, in qb_right.iterall()])

        elif self._operator == '-=':
            entities_collection[set_entity].set = entities_collection[set_entity].set  - set([_ for _, in qb_right.iterall()])
        return entities_collection

class RuleSequence(object):
    pass



class AiidaEntitySet():
    def __init__(self, aiida_type, unique_identifier='id'):
        self._unique_identifier = unique_identifier
        self._aiida_type=aiida_type

        self._set = set()
    def add(self, *args):
        for a in args:
            if isinstance(a, self._aiida_type):
                self._set.add(getattr(a, self._unique_identifier))
            elif isinstance(a, (int, str)):
                self._set.add(a)
    @property
    def set(self):
        return self._set
    @property
    def identifier(self):
        return self._unique_identifier
    @property
    def aiida_type(self):
        return self._aiida_type
    def __add__(self, set_to_add):
        return self.set.union(set_to_add)


    def __sub__(self, set_to_subtract):
        return self.set.difference(set_to_subtract)
class AiidaEntitiesCollection(object):
    def __init__(self, ):
        # TODO: Identifiers
        self.nodes = AiidaEntitySet(Node)
        self.groups = AiidaEntitySet(Group)
        self.computers = AiidaEntitySet(Computer)
        self.users = AiidaEntitySet(User)

    def __getitem__(self, key):
        if key is Node:
            return self.nodes
        elif key is Group:
            return self.groups
        elif key is Computer:
            return self.computers
        elif key is User:
            return self.users
        else:
            raise KeyError(key)
    def copy(self):

        other = self.__class__()
        other.nodes.set = self.nodes.set.copy()
        other.groups.set = self.groups.set.copy()
        other.computers.set = self.computers.set.copy()
        other.users.set = self.users.set.copy()
        return other
        # TODO links

        #~ @property
        #~ def nodes(self):
            #~ return self._nodes
        #~ @property
        #~ def groups(self):
            #~ return self._groups
        #~ @property
        #~ def users(self):
            #~ return self._users
        #~ @property
        #~ def computers(self):
            #~ return self._computers



class GraphExplorer(object):
    def __init__(self, instruction):
        pass
