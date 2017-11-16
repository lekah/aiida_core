import re
from aiida.orm.querybuilder import QueryBuilder
from aiida.orm.node import Node
from aiida.orm.group import Group
from aiida.orm.computer import Computer
from aiida.orm.user import User



BASE_DEFAULT_DICT = {'n':Node, 'u':User, 'g':Group, 'c':Computer}

def _get_aiida_entity_from_string(specifier):
    lowspec = specifier.lower()
    
    if lowspec in BASE_DEFAULT_DICT:
        return BASE_DEFAULT_DICT[lowspec]
    else:
        raise NotImplementedError


RULE_REGEX = re.compile("^[A-Z] [-|+]? = [A-Za-z] (?P<relationship>((<|-) [iorc]? (-|>) [A-Za-z])?) (?P<qbspec>(\\ s)?)$", re.X )

class Rule(object):
    @classmethod
    def get_from_string(cls, string):
        """
        
        """
        pass
        
    def __init__(self, projecting_entity, operator, relationship_entity=None, relationship=None):
        self._projecting_entity = projecting_entity
        self._relationship_entity = relationship_entity
        self._relationship = relationship
        self._operator = operator
    def apply(self, entities_collection):
        """
        N = N <- n
        """
        
        

class RuleSequence(object):
    pass



class AiidaEntitySet(object):
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

class AiidaEntitiesCollection(object):
    def __init__(self, ):
        # TODO: Identifiers
        self._nodes = AiidaEntitiesSet(Node)
        self._groups = AiidaEntitiesSet(Group)
        self._computers = AiidaEntitiesSet(Computer)
        self._users = AiidaEntitiesSet(User)

        # TODO links

        @property
        def nodes(self):
            return self._nodes
        @property
        def groups(self):
            return self._groups
        @property
        def users(self):
            return self._users
        @property
        def computers(self):
            return self._computers



class GraphExplorer(object):
    def __init__(self, instruction):
        pass
