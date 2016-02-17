# -*- coding: utf-8 -*-


# SA querying functionalities


from querybuilder_base import (
    QueryBuilderBase, replacement_dict, datetime, InputValidationError
)

from dummy_model import (
    # Tables:
    DbNode      as DummyNode,
    DbLink      as DummyLink,
    DbAttribute as DummyAttribute,
    DbCalcState as DummyState,
    DbPath      as DummyPath,
    DbUser      as DummyUser,
    DbComputer  as DummyComputer,
    DbGroup     as DummyGroup,
    table_groups_nodes  as Dummy_table_groups_nodes,
    and_, or_, not_, except_, aliased,      # Queryfuncs
    DjangoAiidaNode, DjangoAiidaGroup,      # Aiida classes when using Django
    session,                                # session with DB
)



class QueryBuilder(QueryBuilderBase):
    """
    The QueryBuilder class for the Django backend
    and corresponding schema.
    In order to use SQLAlchemy's querying functionalities, 
    a :func:`~aiida.backends.querybuild.dummy_model`
    was written which can create a session with the aiidadb and
    query the tables defined there.
    """

    def __init__(self, queryhelp, **kwargs):
        self.Link               = DummyLink
        self.Path               = DummyPath
        self.Node               = DummyNode
        self.Computer           = DummyComputer
        self.User               = DummyUser
        self.Group              = DummyGroup
        self.table_groups_nodes = Dummy_table_groups_nodes
        self.AiidaNode          = DjangoAiidaNode
        self.AiidaGroup         = DjangoAiidaGroup
        
        super(QueryBuilder, self).__init__(queryhelp, **kwargs)

    def get_ormclass(self, ormclasstype):
        """
        Return the valid ormclass for the connections
        """
        if ormclasstype == 'group':
            return DummyGroup
        elif ormclasstype == 'computer':
            return DummyComputer
        return DummyNode

    @staticmethod
    def get_session():
        return session

    @staticmethod
    def get_expr(operator, value, column, attr_key):
        """
        :param operator:
            A string representation of a valid operator,
            '==', '<=', '<', '>' ,' >=', 'in', 'like'
            or the negation of such (same string prepended with '~')
        :param value:
            The right part of the expression
        :param column:
            An instance of *sqlalchemy.orm.attributes.InstrumentedAttribute*.
            Giving the left part of the expression
        :param attr_key:
            If I am looking at an attribute, than the attr_key is the key
            to look for in db_dbattributes table.
        
        :returns:
            An SQLAlchemy expression
            (*sqlalchemy.sql.selectable.Exists*,
            *sqlalchemy.sql.elements.BinaryExpression*, etc) 
            that can be evaluated by a query intance.
        """
        
        if operator.startswith('~'):
            negation = True
            operator = operator.lstrip('~')
        else:
            negation = False
        if attr_key:
            mapped_class = column.prop.mapper.class_
            if isinstance(value, str):
                mapped_entity = mapped_class.tval
            elif isinstance(value, bool):
                mapped_entity = mapped_class.bval
            elif isinstance(value, float):
                mapped_entity = mapped_class.fval
            elif isinstance(value, int):
                mapped_entity = mapped_class.ival
            elif isinstance(value, datetime.datetime):
                mapped_entity = mapped_class.dval
            expr = column.any(
                and_(
                    mapped_class.key.like(attr_key),
                    QueryBuilder.get_expr(operator, value, mapped_entity , None)
                )
            )
        else:
            if operator == '==':
                expr = column == value
            elif operator == '>':
                expr = column > value 
            elif operator == '<':
                expr = column < value 
            elif operator == '>=':
                expr = column >= value 
            elif operator == '<=':
                expr = column <= value 
            elif operator == 'like':
                expr = column.like(value)
            elif operator == 'ilike':
                expr = column.like(value)
            elif operator == 'in':
                expr = column.in_(value)
            else:
                raise Exception('Unkown operator %s' % operator)
        if negation:
            return not_(expr)
        return expr

    def analyze_filter_spec(self, alias, filter_spec):
        """
        Recurse through the filter specification and apply filter operations.
        
        :param alias: The alias of the ORM class the filter will be applied on
        :param filter_spec: the specification as given by the queryhelp
        
        :returns: an instance of *sqlalchemy.sql.elements.BinaryExpression*.
        """
        expressions = []
        for path_spec, filter_operation_dict in filter_spec.items():
            if path_spec in  ('and', 'or', '~or', '~and'):
                subexpressions = [
                    analyze_filter_spec(alias, sub_filter_spec)
                    for sub_filter_spec in filter_operation_dict
                ]
                if path_spec == 'and':
                    expressions.append(and_(*subexpressions))
                elif path_spec == 'or':
                    expressions.append(or_(*subexpressions))
                elif path_spec == '~and':
                    expressions.append(not_(and_(*subexpressions)))
                elif path_spec == '~or':
                    expressions.append(not_(or_(*subexpressions)))
            else:
                column_name = path_spec.split('.')[0]
                column =  self.get_column(column_name, alias)
                attr_key = '.'.join(path_spec.split('.')[1:])
                is_attribute = bool(attr_key)
                if not isinstance(filter_operation_dict, dict):
                    filter_operation_dict = {'==':filter_operation_dict}
                [
                    expressions.append(
                        self.get_expr(
                            operator, value, column, attr_key
                        )
                    ) 
                    for operator, value 
                    in filter_operation_dict.items()
                ]
        return and_(*expressions)

    def get_dict(self):
        """
        Returns the json-compatible list
        """
        return {
            'path':self.path, 
            'filters':self.filters,
            'project':self.projection_dict_user
        }


    def add_projectable_entity(self, alias, projectable_spec):
        """
        :param alias: 
            A instance of *sqlalchemy.orm.util.AliasedClass*, alias for an ormclass
        :param projectable_spec:
            User specification of what to project.
            Appends to query's entities what the user want to project
            (have returned by the query)
        
        """
        print type(alias)
        #~ raw_input(projectable_spec)
        if projectable_spec == '*': # project the entity
            self.que = self.que.add_entity(alias)
        else:
            column_name = projectable_spec
            if column_name == 'attributes':
                raise InputValidationError(
                    "\n\nI cannot project on Attribute table\n"
                    "Please use the SA backend for that functionality"
                )
            elif '.' in column_name:
                raise InputValidationError(
                    "\n\n"
                    "I cannot project on other entities\n"
                    "Please use the SA backend for that functionality"
                )
            self.que =  self.que.add_columns(self.get_column(column_name, alias))
        return projectable_spec
