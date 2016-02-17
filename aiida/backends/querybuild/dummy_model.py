# -*- coding: utf-8 -*-

"""
The dummy model encodes the model defined by django in backends.djsite
using SQLAlchemy.
This is done to query the database with more performant ORM of SA.
"""


from sqlalchemy.ext.declarative import declarative_base
from sa_init import (
    Column, Table, ForeignKey,
    Integer, String, DateTime, Float, Boolean, Text,  # basic column types
    UUID, JSONB,                                      # Fancy column types
    UniqueConstraint,aliased,
    select, func, join, and_, or_, not_, except_,     # join and filter ops
    relationship, backref, column_property,           # Table to table relationsships
    sessionmaker, create_engine,                      # connection
    foreign, mapper,
)

# Aiida Django classes:
from aiida.orm.implementation.django.node import Node as DjangoAiidaNode
from aiida.orm.implementation.django.group import Group as DjangoAiidaGroup
from aiida.orm.implementation.calculation import from_type_to_pluginclassname
from aiida.common.pluginloader import load_plugin


# SETTINGS:
from aiida.common.setup import get_profile_config
from aiida.backends import settings

#EXCEPTIONS
from aiida.common.exceptions import DbContentError, MissingPluginError

# MISC
from aiida.backends.sqlalchemy.models.utils import uuid_func
from aiida.utils import timezone



Base = declarative_base()

class DbLink(Base):
    __tablename__ = "db_dblink"
    id = Column(Integer, primary_key=True)
    input_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', deferrable=True, initially="DEFERRED")
    )
    output_id = Column(
        Integer,
        ForeignKey(
            'db_dbnode.id',
            ondelete="CASCADE",
            deferrable=True,
            initially="DEFERRED"
        )
    )
    input = relationship("DbNode", primaryjoin="DbLink.input_id == DbNode.id")
    output = relationship("DbNode", primaryjoin="DbLink.output_id == DbNode.id")
    label = Column(String(255), index=True, nullable=False)

class DbPath(Base):
    __tablename__ = "db_dbpath"
    id = Column(Integer, primary_key=True)
    parent_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', deferrable=True, initially="DEFERRED")
    )
    child_id = Column(
        Integer,
        ForeignKey('db_dbnode.id', deferrable=True, initially="DEFERRED")
    )
    parent = relationship(
        "DbNode",
        primaryjoin="DbPath.parent_id == DbNode.id",
        backref="child_paths"
    )
    child = relationship(
        "DbNode",
        primaryjoin="DbPath.child_id == DbNode.id",
        backref="parent_paths"
    )
    depth = Column(Integer)
    entry_edge_id = Column(Integer)
    direct_edge_id = Column(Integer)
    exit_edge_id = Column(Integer)


class DbNode(Base):
    __tablename__ = "db_dbnode"
    id = Column(Integer, primary_key=True)
    uuid = Column(UUID(as_uuid=True), default=uuid_func)
    type = Column(String(255), index=True)
    label = Column(String(255), index=True, nullable=True)
    description = Column(Text(), nullable=True)
    ctime = Column(DateTime(timezone=True), default=timezone.now)
    mtime = Column(DateTime(timezone=True), default=timezone.now)
    dbcomputer_id = Column(
        Integer,
        ForeignKey('db_dbcomputer.id', deferrable=True, initially="DEFERRED"),
        nullable=True
    )
    dbcomputer = relationship(
        'DbComputer',
        backref=backref('dbnodes', passive_deletes=True)
    )
    user_id = Column(
        Integer,
        ForeignKey('db_dbuser.id', deferrable=True, initially="DEFERRED"),
        nullable=False
    )
    user = relationship('DbUser', backref='dbnodes')

    public = Column(Boolean, default=False)

    nodeversion = Column(Integer, default=1)
    
    attributes = relationship(
        'DbAttribute',
        uselist = True,
    )


    outputs = relationship(
        "DbNode",
        secondary       =   "db_dblink",
        primaryjoin     =   "DbNode.id == DbLink.input_id",
        secondaryjoin   =   "DbNode.id == DbLink.output_id",
        backref         =   backref("inputs", passive_deletes=True),
        passive_deletes =   True
    )

    children = relationship(
        "DbNode",
        secondary       =   "db_dbpath",
        primaryjoin     =   "DbNode.id == DbPath.parent_id",
        secondaryjoin   =   "DbNode.id == DbPath.child_id",
        backref         =   "parents"
    )
    def get_aiida_class(self):
        """
        Return the corresponding instance of
        :func:`~aiida.orm.implementation.django.node.Node`
        or a subclass return by the plugin loader.
        
        .. todo:: 
            The behavior is quite pathetic, creating a django DbNode instance
            to instantiate the aiida instance.
            These means that every time you load Aiida instances with 
            the QueryBuilder when using Django as a backend, three instances
            are instantiated for every Aiida instance you load!
            Could be fixed by allowing DbNode from the dummy nodel to be passed
            to AiidaNode's __init__.

        :returns: An instance of the plugin class
        """
        # I need to import the DbNode in the Django model,
        # and instantiate an object that has the same attributes as self.
        from aiida.backends.djsite.db.models import DbNode as DjangoSchemaDbNode
        dbnode = DjangoSchemaDbNode(
            id = self.id,
            type = self.type,
            uuid = self.uuid,
            ctime = self.ctime,
            mtime = self.mtime,
            label = self.label,
            dbcomputer_id = self.dbcomputer_id,
            user_id = self.user_id,
            public = self.public,
            nodeversion = self.nodeversion
        )
        return dbnode.get_aiida_class()


class DbCalcState(Base):
    __tablename__ = "db_dbcalcstate"
    id = Column(Integer, primary_key=True)
    dbnode = relationship('DbNode', backref=backref('dbstates', passive_deletes=True))
    state = Column(String(255))
    time = Column(DateTime(timezone=True), default=timezone.now)
    dbnode_id = Column(Integer, ForeignKey('db_dbnode.id', ondelete="CASCADE", deferrable=True, initially="DEFERRED"))
    __table_args__ = (
        UniqueConstraint('dbnode_id', 'state'),
    )

class DbAttribute(Base):
    __tablename__ = "db_dbattribute"
    id  = Column(Integer, primary_key = True)
    dbnode_id = Column(Integer, ForeignKey('db_dbnode.id'))
    key = Column(String(255))
    datatype = Column(String(10))
    tval = Column(String, default='')
    fval = Column(Float, default=None, nullable=True)
    ival = Column(Integer, default=None, nullable=True)
    bval = Column(Boolean, default=None, nullable=True)
    dval = Column(DateTime, default=None, nullable = True)

class DbComputer(Base):
    __tablename__ = "db_dbcomputer"

    id = Column(Integer, primary_key=True)

    uuid = Column(UUID(as_uuid=True), default=uuid_func)
    name = Column(String(255), unique=True, nullable=False)
    hostname = Column(String(255))

    description = Column(Text, nullable=True)
    enabled = Column(Boolean)

    transport_type = Column(String(255))
    scheduler_type = Column(String(255))

    transport_params = Column(String(255))

class DbUser(Base):
    __tablename__ = "db_dbuser"

    id = Column(Integer, primary_key=True)
    email = Column(String(254), unique=True, index=True)
    password = Column(String(128))  # Clear text password ?
    first_name = Column(String(254), nullable=True)
    last_name = Column(String(254), nullable=True)
    institution = Column(String(254), nullable=True)

    is_staff = Column(Boolean, default=False)
    is_active = Column(Boolean, default=False)

    last_login = Column(DateTime(timezone=True), default=timezone.now)
    date_joined = Column(DateTime(timezone=True), default=timezone.now)


table_groups_nodes = Table(
    'db_dbgroup_dbnodes',
    Base.metadata,
    Column('id', Integer, primary_key=True),
    Column('dbnode_id', Integer, ForeignKey('db_dbnode.id', deferrable=True, initially="DEFERRED")),
    Column('dbgroup_id', Integer, ForeignKey('db_dbgroup.id', deferrable=True, initially="DEFERRED"))
)

class DbGroup(Base):
    __tablename__ = "db_dbgroup"

    id = Column(Integer, primary_key=True)

    uuid = Column(UUID(as_uuid=True), default=uuid_func)
    name = Column(String(255), index=True)

    type = Column(String(255), default="", index=True)

    time = Column(DateTime(timezone=True), default=timezone.now)
    description = Column(Text, nullable=True)

    user_id = Column(Integer, ForeignKey('db_dbuser.id', ondelete='CASCADE', deferrable=True, initially="DEFERRED"))
    user = relationship('DbUser', backref=backref('dbgroups', cascade='merge'))

    dbnodes = relationship('DbNode', secondary=table_groups_nodes,
                           backref="dbgroups", lazy='dynamic')

    __table_args__ = (
        UniqueConstraint('name', 'type'),
    )

    def __str__(self):
        if self.type:
            return '<DbGroup [type: {}] "{}">'.format(self.type, self.name)
        else:
            return '<DbGroup [user-defined] "{}">'.format(self.name)
    def get_aiida_class(self):
        
        from aiida.backends.djsite.db.models import DbGroup as DjangoSchemaDbGroup
        dbgroup = DjangoSchemaDbGroup(
            id = self.id,
            type = self.type,
            uuid = self.uuid,
            name = self.name,
            time = self.time,
            description  = self.description,
            user_id = self.user_id,
        )

        return DjangoAiidaGroup(dbgroup = dbgroup)



states = select(
        [
            DbCalcState.dbnode_id.label('dbnode_id'),
            func.max(DbCalcState.time).label('lasttime'),
        ]
    ).group_by(DbCalcState.dbnode_id).alias()

recent_states = select([
        DbCalcState.id.label('id'),
        DbCalcState.dbnode_id.label('dbnode_id'),
        DbCalcState.state.label('state'),
        states.c.lasttime.label('time')
    ]).\
    select_from(
        join(
            DbCalcState,
            states,
            and_(
                DbCalcState.dbnode_id == states.c.dbnode_id,
                DbCalcState.time == states.c.lasttime,
            )
        )
    ).alias() # .group_by(DbCalcState.dbnode_id, DbCalcState.time)

state_mapper = mapper(
    DbCalcState,
    recent_states, 
    primary_key= recent_states.c.dbnode_id,
    non_primary=True, 
)

DbNode.state_instance = relationship(
    state_mapper,
    primaryjoin = recent_states.c.dbnode_id == foreign(DbNode.id),
    viewonly=True,
)

DbNode.state = column_property(
    select([recent_states.c.state]).
    where(recent_states.c.dbnode_id == foreign(DbNode.id))
)


config = get_profile_config(settings.AIIDADB_PROFILE)
engine_url = ("postgresql://{AIIDADB_USER}:{AIIDADB_PASS}@"
              "{AIIDADB_HOST}:{AIIDADB_PORT}/{AIIDADB_NAME}").format(**config)
engine = create_engine(engine_url)
Session = sessionmaker(bind=engine)

session = Session()

#~ print session.query(DbAttribute.id, DbAttribute.ival).limit(3).all()
#~ print session.query(DbComputer).all()
#~ print session.query(DbUser).all()
#~ attrs =  session.query(DbNode).filter(DbNode.attributes.any(DbAttribute.key.like('%kinds'))).first().attributes
#~ for attr in attrs:
    #~ print attr.key
