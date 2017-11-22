import re

from aiida.orm.querybuilder import QueryBuilder
from aiida.orm.node import Node
from aiida.orm.group import Group
from aiida.orm.computer import Computer
from aiida.orm.user import User



BASE_DEFAULT_DICT = {'n':Node, 'u':User, 'g':Group, 'c':Computer}

VALID_RELATIONSHIPS = {'--', '<-', '->', '>>', '<<', '', None}
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



class StashCommit(object):
    def apply(self, stash, collection):
        stash.append(collection)
        return collection.copy()

class StashPop(object):
    def apply(self, stash, collection):
        return stash.pop(-1)

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


    @property
    def only_update(self):
        """
        :returns: A boolean, whether the Rule is an UpdateRule. This allows for tricks in the RuleSequence!
        """
        return self._operator.startswith('+')


    def apply(self, entities_collection):
        """
        N = N <- n
        """

        def get_entity_n_filters(entity, additional_filters):
            if isinstance(entity, SubSetOfDB):
                aiida_type = entity.aiida_type
                filters={entities_collection[aiida_type].identifier:{'in':entities_collection[aiida_type].get_keys()}}
            else:
                aiida_type = entity
                filters = {}
            return aiida_type, filters, [entities_collection[aiida_type].identifier]

            # Todo add filters from spec!
        set_entity, set_filters, _ = get_entity_n_filters(self._set_entity, {})
        qb_left = QueryBuilder().append(set_entity, filters=set_filters)


        # qb_list_right is a list of QueryBuilder instances
        # it has to be a list because some relationships can only be made with
        # several querybuilder instances
        # i.e. N--n (node connected to other node regardless of direction!
        qb_right = [QueryBuilder()]
        proj_entity, proj_filters, proj_project = get_entity_n_filters(self._projecting_entity, {})
        qb_right[0].append(proj_entity, filters=proj_filters, project=proj_project, tag='p')
        if self._relationship:
            if not self._relationship_entity:
                raise Exception() # should be before, no
            rlshp_entity, rlshp_filters, _ = get_entity_n_filters(self._relationship_entity, {})
            if issubclass(proj_entity, Node):
                if issubclass(rlshp_entity, Node):
                    if self._relationship == '<-':
                        qb_right[0].append(rlshp_entity, input_of='p', filters=rlshp_filters)
                    elif self._relationship == '->':
                        qb_right[0].append(rlshp_entity, output_of='p', filters=rlshp_filters)
                    elif self._relationship == '>>':
                        qb_right[0].append(rlshp_entity, descendant_of='p', filters=rlshp_filters)
                    elif self._relationship == '<<':
                        qb_right[0].append(rlshp_entity, ancestor_of='p', filters=rlshp_filters)
                    elif self._relationship == '--':
                        # A bit of QueryBuilder magic
                        qb_right.append(qb_right[0].copy())
                        qb_right[0].append(rlshp_entity, output_of='p', filters=rlshp_filters)
                        qb_right[1].append(rlshp_entity, input_of='p', filters=rlshp_filters)
                    else:
                        raise NotImplementedError
                elif issubclass(rlshp_entity, Group):
                    if self._relationship == '--':
                        qb_right[0].append(rlshp_entity, group_of='p', filters=rlshp_filters)
                    else:
                        raise NotImplementedError

                elif issubclass(rlshp_entity, User):
                    if self._relationship == '--':
                        qb_right[0].append(rlshp_entity, creator_of='p', filters=rlshp_filters)
                    else:
                        raise NotImplementedError
                elif issubclass(rlshp_entity, Computer):
                    if self._relationship == '--':
                        qb_right[0].append(rlshp_entity, computer_of='p', filters=rlshp_filters)
                    else:
                        raise NotImplementedError
                else:
                    raise NotImplementedError
            elif issubclass(proj_entity, Group):
                if issubclass(rlshp_entity, Node):
                    if self._relationship == '--':
                        qb_right[0].append(rlshp_entity, member_of='p', filters=rlshp_filters)
                    else:
                        raise NotImplementedError

            elif issubclass(proj_entity, User):
                if issubclass(rlshp_entity, Node):
                    if self._relationship == '--':
                        qb_right[0].append(rlshp_entity, created_by='p', filters=rlshp_filters)
                    else:
                        raise NotImplementedError

            elif issubclass(proj_entity, Computer):
                if issubclass(rlshp_entity, Node):
                    if self._relationship == '--':
                        qb_right[0].append(rlshp_entity, has_computer='p', filters=rlshp_filters)
                    else:
                        raise NotImplementedError
            else:
                raise NotImplementedError

        qb_right_result = set([_ for qb in qb_right for _, in qb.iterall()])
        if self._operator == '=':
            #~ print entities_collection[set_entity].set
            #~ print qb_right.all()
            entities_collection[set_entity]._set_key_set_nocheck(qb_right_result)

        elif self._operator == '+=':
            entities_collection[set_entity]._set_key_set_nocheck(entities_collection[set_entity]._set.union(qb_right_result))

        elif self._operator == '-=':
            entities_collection[set_entity]._set_key_set_nocheck(entities_collection[set_entity]._set.difference(qb_right_result))
        return entities_collection



class RuleSequence(object):
    """
    A RuleSequence is - as the name suggests - a sequence of rules.
    *   I want to get all nodes that belong to all groups that the nodes in my set belong to: G=g--N N=n--G
    *   I want to get the inputs of the inputs: N=n->N N=n->N  or (N=n->N)2
    *   I want to get all ancestors!
    """

    @classmethod
    def get_from_string(cls, string):
        pass

    def __init__(self, *rules, **kwargs):
        """
        :param *rules: Instances of Rule, RuleSequence, StashPop and StashCommit that will be executed in that order
        :param
        """
        for r in rules:
            if not isinstance(r, (Rule, RuleSequence, StashCommit, StashPop)):
                raise ValueError("{} {} is not a valid input".format(type(r), r))

        niter = kwargs.pop('niter', 1)
        self.set_niter(niter)


        if kwargs:
            raise Exception("Unrecognized keywords {}".format(kwargs.keys()))
        self._rules = rules
        self._niter = niter
        self._last_niter = None
        self._stash = []


    @property
    def only_update(self):
        """
        :returns: A boolean, whether the Rule is an UpdateRule. This allows for tricks in the RuleSequence!
        """
        return all([r.only_update for r in self._rules])

    def apply(self, main_collection):
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

        visited_collection = collection
        # I also have an operation_collection. This collection is being updated by the rule
        # at the end of every loop, I put everything
        operational_collection = collection.copy()

        #~ new_collection = operational_collection.copy()
        #~ visited_collection = AiidaEntitiesCollection()
        while True:
            if iterations == self._niter:
                break
            if not operational_collection:
                break
            #~ operational_collection = new_collection.copy()
            for item in self._rules:
                if isinstance(item, (Rule, RuleSequence)):
                    # I change the operational collection in place!
                    operational_collection = item.apply(operational_collection)
                elif isinstance(item, (StashCommit, StashPop)):
                    operational_collection = item.apply(operational_collection, self._stash)
                else:
                    assert(True, "Should not get here")
                #~ print operational_collection.nodes._set
            # Now I update the visited collection which is the collection I keep track of:
            # So, what here is actually new?
            #~ operational_collection = operational_collection - visited_collection
            operational_collection -= visited_collection
            #~ visited_collection = visited_collection + operational_collection
            visited_collection += operational_collection
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
                if isinstance(item, (Rule, RuleSequence)):
                    # I change the operational collection in place!
                    operational_collection = item.apply(operational_collection)
                elif isinstance(item, (StashCommit, StashPop)):
                    operational_collection = item.apply(operational_collection, self._stash)
                else:
                    assert(True, "Should not get here")
            iterations += 1
        self._last_niter = iterations
        return operational_collection

    def get_last_niter(self):
        return self._last_niter

    def set_niter(self, niter):
        if not isinstance(niter, int):
            raise ValueError("niter has to be an integer")
        self._niter = niter




class AiidaEntitySet(object):
    def __init__(self, aiida_type, unique_identifier='id'):
        self._aiida_type=aiida_type
        self._set = set()
        # I'm hardcoding the unique identifier type here, since that's straightforward
        if unique_identifier == 'id':
            self._unique_identifier_type = int
        elif unique_identifier == 'uuid':
            self._unique_identifier_type = (str, unicode)
        # TODO: email for users? what else is a valid unique identifier?
        else:
            raise ValueError("Unknown unique identifier {}".format(unique_identifier))
        self._unique_identifier = unique_identifier

    @property
    def identifier(self):
        return self._unique_identifier

    @property
    def aiida_type(self):
        return self._aiida_type

    def set_aiida_types(self, *args):
        new_keys = set()
        for a in args:
            if isinstance(a, self._aiida_type):
                new_keys.add(getattr(a, self._unique_identifier))
            else:
                raise Exception("Don't know what to do with {} {}".format(type(a), a))
        self._set = new_keys

    def add_aiida_types(self, *args):
        for a in args:
            if isinstance(a, self._aiida_type):
                self._set.add(getattr(a, self._unique_identifier))
            else:
                raise Exception("Don't know what to do with {} {}".format(type(a), a))

    def set_keys(self, *keys):
        new_keys = set()
        for k in keys:
            if isinstance(k, self._unique_identifier_type):
                new_keys.add(k)
            else:
                raise Exception("Don't know what to do with {} {}".format(type(k), k))
        self._set = new_keys

    def add_keys(self, *keys):
        for k in keys:
            if isinstance(k, self._unique_identifier_type):
                self._set.add(k)
            else:
                raise Exception("Don't know what to do with {} {}. I accept {}".format(type(k), k, self._unique_identifier_type))

    def get_keys(self):
        return self._set

    def _set_key_set_nocheck(self, _set):
        self._set = _set

    def copy(self):
        new = AiidaEntitySet(aiida_type=self.aiida_type, unique_identifier=self.identifier)
        # TODO: Avoid the check to speed up?
        new._set_key_set_nocheck(self._set.copy())
        return new


    def _check_self_and_other(self, other):
        if self.aiida_type != other.aiida_type:
            raise Exception("The two instances do not have the same aiida type!")
        if self.identifier != other.identifier:
            raise Exception("The two instances do not have the same identifier!")
        return True

    def __add__(self, other):
        self._check_self_and_other(other)
        new = AiidaEntitySet(self.aiida_type, unique_identifier=self.identifier)
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
        new = AiidaEntitySet(self.aiida_type, unique_identifier=self.identifier)
        new._set_key_set_nocheck(self._set.difference(other._set))
        return new

    def __isub__(self, other):
        """
        Adding inplace!
        """
        self._check_self_and_other(other)
        self._set = self._set.difference(other._set)
        return self

    def __len__(self):
        return len(self._set)

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

    def __len__(self):
        return sum([len(_) for _ in self.values()])

    def __add__(self, other):
        new = AiidaEntitiesCollection()
        new.nodes = self.nodes + other.nodes
        new.groups = self.groups + other.groups
        new.computers = self.computers + other.computers
        new.users = self.users + other.users
        return new


    def __iadd__(self, other):
        self.nodes += other.nodes
        self.groups += other.groups
        self.computers +=  other.computers
        self.users +=  other.users
        return self


    def __sub__(self, other):
        new = AiidaEntitiesCollection()
        new.nodes = self.nodes - other.nodes
        new.groups = self.groups - other.groups
        new.computers = self.computers - other.computers
        new.users = self.users - other.users
        return new

    def __isub__(self, other):
        self.nodes -= other.nodes
        self.groups -= other.groups
        self.computers -=  other.computers
        self.users -= other.users
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
    def __init__(self, instruction):
        pass
