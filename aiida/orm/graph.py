import re, copy, collections


from aiida.orm.querybuilder import QueryBuilder
from aiida.orm import (
    Node, Group, Computer, User, Code, Calculation, Data,
    CalculationFactory, DataFactory)

from aiida.common.hashing import make_hash
from aiida.common.extendeddicts import Enumerate
from frozendict import frozendict


# I don't want anything to be hard-coded, so I define all strings that are used in this
# module here as frozensets or frozendicts
# The key is always a valid reference, that can be defined by the user

# All the aiida entities I know about will be referenced by the following strings:
entities = Enumerate(('NODE','GROUP','COMPUTER', 'USER'))
subentities = Enumerate(( 'DATA', 'CALCULATION', 'CODE'))
factories = Enumerate(('DATAFACTORY', 'CALCULATIONFACTORY'))
operators = Enumerate(('UPDATE', 'ASSIGN', 'REMOVE'))
relationships = Enumerate(('LINKED','LEFTLINKED', 'RIGHTLINKED', 'LEFTPATH', 'RIGHTPATH'))

OPERATOR_SYMBOLS = frozendict({
    '+=':operators.UPDATE,
    '=':operators.ASSIGN,
    '-=':operators.REMOVE})

RELATIONSHIP_SYMBOLS = frozendict({
    '--':relationships.LINKED,
    '<-':relationships.LEFTLINKED,
    '->':relationships.RIGHTLINKED,
    '<<':relationships.LEFTPATH,
    '>>':relationships.RIGHTPATH})

ENTITY_ABBREVIATIONS = frozendict({
    'N':entities.NODE,
    'G':entities.GROUP,
    'U':entities.USER,
    'C':entities.COMPUTER,})

SUBENTITY_ABBREVIATIONS = frozendict({
    'DA':subentities.DATA,
    'CA':subentities.CALCULATION,
    'CO':subentities.CODE,})


FACTORY_ABBREVATIONS = frozendict({
    'DF':factories.DATAFACTORY,
    'CF':factories.CALCULATIONFACTORY})

class SubSetOfDB(object):
    def __init__(self, aiida_type):
        self._aiida_type = aiida_type

    def __str__(self):
        return 'SubSet of {}'.format(str(self._aiida_type))
    @property
    def aiida_type(self):
        return self._aiida_type


ENTITY_MAP = frozendict({
    entities.NODE:Node,
    entities.GROUP:Group,
    entities.USER:User,
    entities.COMPUTER:Computer,
    subentities.CALCULATION:Calculation,
    subentities.DATA:Data,
    subentities.CODE:Code,
    factories.DATAFACTORY:DataFactory,
    factories.CALCULATIONFACTORY:CalculationFactory,
})

# BASE_DEFAULT_DICT = {'n':Node, 'u':User, 'g':Group, 'c':Computer}

#~ VALID_RELATIONSHIPS = {'--', '<-', '->', '>>', '<<', '', None}
#~ VALID_OPERATORS = {'=', '+=', '-=', None}

NODE2NODE = 'node2node'
NODE2GROUP = 'node2group'
NODE2COMPUTER = 'node2computer'
NODE2USER = 'node2user'



def _get_relationship(specifier):
    # TODO: More specs for specifying link types as -oi-
    if specifier == "":
        return None
    elif specifier in relationships:
        return specifier
    elif specifier in RELATIONSHIP_SYMBOLS:
        return RELATIONSHIP_SYMBOLS[specifier]
    else:
        raise ValueError("|{}| is not a valid relationship\nValid specifiers are: {}"
        "".format(specifier, list(relationships)+RELATIONSHIP_SYMBOLS.keys()))

def _get_operator(specifier):
    # TODO: More specs for specifying link types as -oi-
    if specifier == "":
        return None
    elif specifier in operators:
        return specifier
    elif specifier in OPERATOR_SYMBOLS:
        return OPERATOR_SYMBOLS[specifier]
    else:
        raise ValueError("|{}| is not a valid relationship\nValid specifiers are: {}".format(specifier, VALID_RELATIONSHIPS))


RULE_REGEX = re.compile("""
    ^(?P<set_entity>[A-Z])
    (?P<operator>[-|+]? =)
    (?P<projecting_entity>[A-Za-z])
    (?P<relationship>((<|-|>) [iorc]? (<|-|>))?)
    (?P<relationship_entity>[A-Za-z]*)
    (?P<qbspec>(\\ s)?)$""", re.X )



class StashCommit(object):
    only_update = True
    def __str__(self):
        return '['
    def apply(self, collection, stash):
        stash.append(collection)
        return collection.copy()

class StashPop(object):
    only_update = True
    def __str__(self):
        return ']'
    def apply(self, collection, stash):
        return stash.pop(-1)

class Operation(object):
    @classmethod
    def get_from_string(cls, string):
        """

        """
        def _get_entity_from_string(specifier):
            upspec = specifier.upper()
            # If nothing was defined:
            if upspec == "":
                return None
            if upspec in ENTITY_ABBREVIATIONS.keys():
                aiida_type = ENTITY_MAP[ENTITY_ABBREVIATIONS[upspec]]
            elif upspec in SUBENTITY_ABBREVIATIONS.keys():
                aiida_type = ENTITY_MAP[SUBENTITY_ABBREVIATIONS[upspec]]
            elif upspec in ENTITY_MAP:
                aiida_type = ENTITY_MAP[upspec]
            else:
                raise NotImplementedError("<{}>".format(specifier))

            # Now, if something was provided with lowercase symbols:
            if specifier.islower():
                return aiida_type
            else:
                return SubSetOfDB(aiida_type)

        match = RULE_REGEX.search(string)
        if match is None:
            raise ValueError("{} is not a valid rule string".format(string))
        return cls(
            set_entity=_get_entity_from_string(match.group('set_entity')),
            operator=_get_operator(match.group('operator')),
            projecting_entity=_get_entity_from_string(match.group('projecting_entity')),
            relationship=_get_relationship(match.group('relationship')),
            relationship_entity=_get_entity_from_string(match.group('relationship_entity')),
            string=string
        )

    def __init__(self, set_entity, projecting_entity, operator,
            relationship_entity=None, relationship=None, link_tracking=True, string=None,
            projection_filters=None, relationship_filters=None, qb_kwargs=None):
        if not isinstance(set_entity, SubSetOfDB):
            raise ValueError()
        self._set_entity = set_entity
        self._projecting_entity = projecting_entity
        self._relationship_entity = relationship_entity
        self._relationship = relationship
        self._operator = operator
        self.set_link_tracking(link_tracking)
        self._string = string
        self.set_projection_filters(projection_filters)
        self.set_relationship_filters(relationship_filters)
        self.set_qb_kwargs(qb_kwargs)

    def __str__(self):
        #~ return st(self._set_entity, self._projecting_entity
        return ' '.join((str(self._set_entity), self._operator, str(self._projecting_entity), self._relationship, str(self._relationship_entity)))

    @property
    def only_update(self):
        """
        :returns: A boolean, whether the Operation is an UpdateRule. This allows for tricks in the RuleSequence!
        """
        return self._operator == operators.UPDATE

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
    def apply(self, entities_collection):
        """
        N = N <- n
        """
        def get_entity_n_filters(entity, entities_collection, additional_filters):
            """
            TODO Docstring, and move out of here!
            """
            if isinstance(entity, SubSetOfDB):
                aiida_type = entity.aiida_type
                filters={entities_collection[aiida_type].identifier:{'in':entities_collection[aiida_type].get_keys()}}
            else:
                aiida_type = entity
                filters = {}
            # todo checks
            if additional_filters is None:
                pass
            elif isinstance(additional_filters, dict):
                filters.update(additional_filters)
            else:
                raise ValueError("additional filters has to be None or dict")
            return aiida_type, filters, [entities_collection[aiida_type].identifier]

        def get_relation_projections(entities_collection, projecting_entity, relationship_entity, relationship):
            # TODO:  some checks
            if issubclass(projecting_entity, Node):
                if issubclass(relationship_entity, Node):
                    if relationship in ('>>', '<<'):
                        #~ edge_projections = ('depth',)
                        assert(True, "")
                    else:
                        edge_projections = ('label', 'type', 'id')
                elif issubclass(relationship_entity, Group):
                    edge_projections = None
                elif issubclass(relationship_entity, (Computer, User)):
                    edge_projections = None
                else:
                    assert(True, "")
            elif issubclass(projecting_entity, Group):
                if issubclass(relationship_entity, Node):
                    edge_projections = None
                else:
                    assert(True, "")
            elif issubclass(projecting_entity, (Computer, User)):
                if issubclass(relationship_entity, Node):
                    edge_projections = None
                else:
                    assert(True, "")
            else:
                assert(True, "")
            return entities_collection[relationship_entity].identifier, edge_projections


        # Ok, here I need to make some strategic decisions
        # 1) Am I tracking links?
        tracking = self._link_tracking, isinstance(self._relationship_entity, SubSetOfDB)
        #~ print 0, tracking,  self._relationship
        # 2) Can I actually track links or is this futile here?
        # 2a) Am I querying a relationship
        if tracking:
            if not self._relationship:
                tracking = False
            # 2b Am I adding the nodes that I'm querying in the relationship to the set or not?
            # That's only the case if projecting nodes are in the db in the relationship nodes in my subset
            elif not isinstance(self._relationship_entity, SubSetOfDB):
                tracking = False
            # It seems ok to track things even if the nodes are already on the set. I.e. to get more
            # relationships!
            #~ elif isinstance(self._projecting_entity, SubSetOfDB):
                #~ tracking = False
            elif self._operator != operators.UPDATE:
                tracking = False
            elif self._relationship not in (relationships.LINKED, relationships.LEFTLINKED, relationships.RIGHTLINKED):
                # I can't deal with
                tracking = False
            else:
                tracking = True

        set_entity, set_filters, _ = get_entity_n_filters(self._set_entity, entities_collection, {})
        qb_left = QueryBuilder().append(set_entity, filters=set_filters)


        # qb_list_right is a list of QueryBuilder instances
        # it has to be a list because some relationships can only be made with
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
                orders = []
                edge_identifiers = []

            else:
                rlshp_project = None
                edge_project = None
            if issubclass(proj_entity, Node):
                if issubclass(rlshp_entity, Node):
                    edge_name = NODE2NODE
                    if self._relationship == relationships.LEFTLINKED:
                        qb_right[0].append(rlshp_entity, input_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0,2,3))
                            edge_identifiers.append(4)
                    elif self._relationship == relationships.RIGHTLINKED:
                        qb_right[0].append(rlshp_entity, output_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1,2,3))
                            edge_identifiers.append(4)
                    elif self._relationship == relationships.RIGHTPATH:
                        qb_right[0].append(rlshp_entity, descendant_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                    elif self._relationship == relationships.LEFTPATH:
                        qb_right[0].append(rlshp_entity, ancestor_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                    elif self._relationship == relationships.LINKED:

                        qb_right.append(qb_right[0].copy())
                        qb_right[0].append(rlshp_entity, output_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        qb_right[1].append(rlshp_entity, input_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1,2,3))
                            orders.append((1,0,2,3))
                            edge_identifiers.append(4)
                            edge_identifiers.append(4)
                    else:
                        raise NotImplementedError
                elif issubclass(rlshp_entity, Group):
                    edge_name = NODE2GROUP
                    if self._relationship == relationships.LINKED:
                        qb_right[0].append(rlshp_entity, group_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1))
                            edge_identifiers.append(None)
                    else:
                        raise NotImplementedError

                elif issubclass(rlshp_entity, User):
                    edge_name = NODE2USER
                    if self._relationship == relationships.LINKED:
                        qb_right[0].append(rlshp_entity, creator_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1))
                            edge_identifiers.append(None)
                    else:
                        raise NotImplementedError
                elif issubclass(rlshp_entity, Computer):
                    edge_name = NODE2COMPUTER
                    if self._relationship == relationships.LINKED:
                        qb_right[0].append(rlshp_entity, computer_of='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((0,1))
                            edge_identifiers.append(None)
                    else:
                        raise NotImplementedError
                else:
                    raise NotImplementedError
            elif issubclass(proj_entity, Group):
                edge_name = NODE2GROUP
                if issubclass(rlshp_entity, Node):
                    #~ print 'HERE'
                    if self._relationship == relationships.LINKED:
                        qb_right[0].append(rlshp_entity,
                                member_of='p', filters=rlshp_filters,
                                project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0))
                            edge_identifiers.append(None)
                    else:
                        raise NotImplementedError
                    #~ print 6, qb_right[0].count()
            elif issubclass(proj_entity, User):
                edge_name = NODE2USER
                if issubclass(rlshp_entity, Node):
                    if self._relationship == relationships.LINKED:
                        qb_right[0].append(rlshp_entity, created_by='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0))
                            edge_identifiers.append(None)
                    else:
                        raise NotImplementedError

            elif issubclass(proj_entity, Computer):
                edge_name = NODE2COMPUTER
                if issubclass(rlshp_entity, Node):
                    if self._relationship == relationships.LINKED:
                        qb_right[0].append(rlshp_entity, has_computer='p', filters=rlshp_filters, project=rlshp_project, edge_project=edge_project)
                        if tracking:
                            orders.append((1,0))
                            edge_identifiers.append(None)
                    else:
                        raise NotImplementedError
            else:
                raise NotImplementedError

        #~ print 7, tracking
        if tracking:
            #~ projection_key, = proj_project
            #~ res = [qb.dict() for qb in qb_right]
            #~ qb_right_result_list = [_ for qb in qb_right for _ in qb.iterall()]
            #~ qb_right_result = [_[0] for  _ in qb_right_result_list]
            #~ qb_right_result = set([row  ['p'][projection_key] for subres in res for row in subres])
            res = [qb.all() for qb in qb_right]
            qb_right_result = [_[0] for  subres in res for _ in subres]
        else:
            qb_right_result = set([_ for qb in qb_right for _, in qb.iterall()])
        #~ print 8, qb_right_result, self._operator
        if self._operator == operators.ASSIGN:
            #~ print entities_collection[set_entity].set
            #~ print qb_right.all()
            entities_collection[set_entity]._set_key_set_nocheck(qb_right_result)

        elif self._operator == operators.UPDATE:
            entities_collection[set_entity]._set_key_set_nocheck(entities_collection[set_entity]._set.union(qb_right_result))
            #~ print 9, entities_collection[set_entity]._set
            if tracking:
                for o, subres,identifier in zip(orders, res, edge_identifiers):
                    for row in subres:
                        if identifier:
                            edge_id = row[identifier]
                        else:
                            edge_id = make_hash(row)
                        #~ print edge_id
                        entities_collection[edge_name]._add_link_dict_no_check({edge_id:tuple(row[i] for i in o)})

        elif self._operator == operators.REMOVE:
            entities_collection[set_entity]._set_key_set_nocheck(entities_collection[set_entity]._set.difference(qb_right_result))
        else:
            raise Exception("Unknown operator <{}>".format(self._operator))
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
        string = string.strip()
        if RULE_REGEX.search(string):
            # You have to try and strip stuff here TODO
            return RuleSequence(Operation.get_from_string(string))


        rules = []
        escaping = False

        commits_to_stash_counter = 0
        news = ""
        s = iter(string)
        #~ print
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
                        rules.append(RuleSequence.get_from_string(news))
                        news = ""
                    if c == ' ':
                        pass
                    elif c == '[':
                        rules.append(StashCommit())
                        commits_to_stash_counter += 1
                    elif c == ']':
                        if commits_to_stash_counter == 0:
                            raise Exception("You're popping more times from stash than you commit")
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
                            raise Exception("Brackets not closed")
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
                rules.append(RuleSequence.get_from_string(news))
        #~ print rules
        return RuleSequence(*rules)

    def __str__(self):
        return '({}){}'.format( ' '.join([str(r) for r in self._rules]), self._niter)

    def __init__(self, *rules, **kwargs):
        """
        :param *rules: Instances of Operation, RuleSequence, StashPop and StashCommit that will be executed in that order
        :param
        """
        for r in rules:
            if not isinstance(r, (Operation, RuleSequence, StashCommit, StashPop)):
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
            #~ print 1, collection.nodes.get_keys()

            if iterations == self._niter:
                break
            if not operational_collection:
                break
            #~ operational_collection = new_collection.copy()
            for item in self._rules:
                #~ print '#1', operational_collection.nodes.get_keys()
                #~ print 2, item
                if isinstance(item, (Operation, RuleSequence)):
                    # I change the operational collection in place!
                    operational_collection = item.apply(operational_collection)
                elif isinstance(item, (StashCommit, StashPop)):
                    operational_collection = item.apply(operational_collection, self._stash)
                else:
                    assert(True, "Should not get here")
                #~ print 3
                # I update here.
                # TODO: Tricks with checking whether Stashe etc to avoid number of updates?
                visited_collection += operational_collection
                #~ print 'H:', item
                #~ print '#2', operational_collection.nodes.get_keys()
                #~ print operational_collection.nodes._set

            # Now I update the visited collection which is the collection I keep track of:
            # So, what here is actually new?
            #~ operational_collection = operational_collection - visited_collection
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
                if isinstance(item, (Operation, RuleSequence)):
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



class AiidaLinkCollection(object):
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
        for k in other.links:
            self._links.pop(k, None)
        return self


class AiidaEntitiesCollection(object):
    """
    Basically an AiiDA subgraph
    """
    def __init__(self, ):
        # TODO: Identifiers
        self.nodes = AiidaEntitySet(Node)
        self.groups = AiidaEntitySet(Group)
        self.computers = AiidaEntitySet(Computer)
        self.users = AiidaEntitySet(User)
        # The following are for the edges,
        # for now, dictionaries with the link_id being the key!
        # the values can be defined as they want, as long as it's consistent...
        # for now I chose tuples from all the stuff I can get
        # entry_in, entry_out, ...

        self.nodes_nodes = AiidaLinkCollection()
        self.nodes_groups = AiidaLinkCollection()
        self.nodes_computers = AiidaLinkCollection()
        self.nodes_users = AiidaLinkCollection()

    def __getitem__(self, key):
        if key is Node:
            return self.nodes
        elif key is Group:
            return self.groups
        elif key is Computer:
            return self.computers
        elif key is User:
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
        new = AiidaEntitiesCollection()
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
        new = AiidaEntitiesCollection()
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

        self._entities_collection = AiidaEntitiesCollection()
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
    #~ links[link_id] = draw_link_settings(inp.pk, node.pk, link_label, link_type)
    nodes = {}
    links = {}
    groups = {}

    #~ print entities_collection.nodes.identifier

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
        # assuming this is a valid key:
        origin_node  = load_node(origin_node)

    if origin_node is not None:
        nodes[origin_node.pk] = draw_node_settings(origin_node, style='filled', color='lightblue')
    with open(fname, 'w') as fout:
        fout.write("digraph G {\n")
        #~ print "digraph G {"
        for _, groupspec in groups.iteritems():
            #~ print groupspec
            #~ fout.write('    ')
            fout.write(groupspec)
        for l_name, l_values in links.iteritems():
            fout.write('    {}\n'.format(l_values))
            #~ print l_values
        for n_name, n_values in nodes.iteritems():
            fout.write("    {}\n".format(n_values))
            #~ print n_values
        #~ for n_name, n_values in additional_nodes.iteritems():
            #~ fout.write("    {}\n".format(n_values))
        fout.write("}\n")
        #~ print '}'

    # Now I am producing the output file


    output_file_name = "{0}.{format}".format(filename or origin_node.pk, format=format)
    exit_status = os.system('dot -T{format} {0} -o {1}'.format(fname, output_file_name, format=format))
    # cleaning up by removing the temporary file
    #~ os.remove(fname)
    return exit_status, output_file_name
