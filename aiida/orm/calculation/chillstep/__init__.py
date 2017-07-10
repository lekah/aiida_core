import traceback

from aiida.orm import JobCalculation, Calculation,Data, Code
from aiida.common.exceptions import InputValidationError, ModificationNotAllowed
from abc import abstractmethod
from aiida.backends.utils import get_automatic_user
from aiida.orm.querybuilder import QueryBuilder
from aiida.common.datastructures import calc_states, sort_states
from aiida.backends import settings
from aiida.backends.utils import BACKEND_DJANGO, BACKEND_SQLA
from aiida.common.links import LinkType



def _set_state_dj(self, state):
    """
    Set the state of the calculation.

    Set it in the DbCalcState to have also the uniqueness check.
    Moreover (except for the IMPORTED state) also store in the 'state'
    attribute, useful to know it also after importing, and for faster
    querying.

    .. todo:: Add further checks to enforce that the states are set
       in order?

    :param state: a string with the state. This must be a valid string,
      from ``aiida.common.datastructures.calc_states``.
    :raise: ModificationNotAllowed if the given state was already set.
    """
    from django.db import transaction, IntegrityError

    from aiida.common.datastructures import sort_states
    from aiida.backends.djsite.db.models import DbCalcState

    if not self.is_stored:
        raise ModificationNotAllowed("Cannot set the calculation state "
                                     "before storing")

    if state not in calc_states:
        raise ValueError(
            "'{}' is not a valid calculation status".format(state))

    old_state = self.get_state()
    if old_state:
        state_sequence = [state, old_state]

        # sort from new to old: if they are equal, then it is a valid
        # advance in state (otherwise, we are going backwards...)
        if sort_states(state_sequence) != state_sequence:
            raise ModificationNotAllowed("Cannot change the state from {} "
                                         "to {}".format(old_state, state))

    try:
        with transaction.atomic():
            new_state = DbCalcState(dbnode=self.dbnode, state=state).save()
    except IntegrityError:
        raise ModificationNotAllowed(
            "Calculation pk= {} already transited through "
            "the state {}".format(self.pk, state))

    # For non-imported states, also set in the attribute (so that, if we
    # export, we can still see the original state the calculation had.
    if state != calc_states.IMPORTED:
        self._set_attr('state', state)

def get_state_dj(self, from_attribute=False):
    """
    Get the state of the calculation.

    .. note:: this method returns the NOTFOUND state if no state
      is found in the DB.

    .. note:: the 'most recent' state is obtained using the logic in the
      ``aiida.common.datastructures.sort_states`` function.

    .. todo:: Understand if the state returned when no state entry is found
      in the DB is the best choice.

    :param from_attribute: if set to True, read it from the attributes
      (the attribute is also set with set_state, unless the state is set
      to IMPORTED; in this way we can also see the state before storing).

    :return: a string. If from_attribute is True and no attribute is found,
      return None. If from_attribute is False and no entry is found in the
      DB, return the "NOTFOUND" state.
    """
    from aiida.backends.djsite.db.models import DbCalcState

    if from_attribute:
        return self.get_attr('state', None)
    else:
        if not self.is_stored:
            return calc_states.NEW
        else:
            this_calc_states = DbCalcState.objects.filter(
                dbnode=self).values_list('state', flat=True)
            if not this_calc_states:
                return None
            else:
                try:
                    most_recent_state = sort_states(this_calc_states)[0]
                except ValueError as e:
                    raise DbContentError("Error in the content of the "
                                         "DbCalcState table ({})".format(
                        e.message))

                return most_recent_state



class Context(object):
    def __init__(self, node):
        super(Context, self).__setattr__('_node', node)

    def __setattr__(self, name, value):
        self._node._set_attr(name, value)
    def __getattr__(self, name):
        return self._node.get_attr(name)
    def __delattr__(self, name):
        self._node._del_attr(name)
    
class ChillstepCalculation(Calculation):

    def __init__(self, dbnode=None, **kwargs):
        # Everything that is input has to be Data
        super(ChillstepCalculation, self).__init__(dbnode=dbnode)
        for k, v in kwargs.items():
            if not isinstance(v, (Code, Data)):
                raise InputValidationError("Input to a Chillstep has to be Data or Code")
            # These are the inputs:
            self.add_link_from(v, label=k, link_type=LinkType.INPUT)
        self.ctx = Context(self)
        if dbnode is None:
            #This was just created:
            self.goto(self.start)

    @property
    def inputs(self):
        return self.inp
    def goto(self, func):
        #~ self._set_attr('_next', func.__name__)
        self.ctx._next = func.__name__
    @abstractmethod
    def start(self):
        print "starting"
    def exit(self):
        pass

    def submit(self):
        if self._to_be_stored:
            self.store_all()
        self._set_state(calc_states.WITHSCHEDULER)


    def _validate(self):
        ###
        super(Calculation, self)._validate()

    def _set_state(self, state):
        if settings.BACKEND == BACKEND_DJANGO:
            _set_state_dj(self, state)
        elif settings.BACKEND == BACKEND_SQLA:
            _set_state_sqla(self, state)
        else:
            raise Exception("unknown backend {}".format(settings.BACKEND))
    def get_state(self):
        if settings.BACKEND == BACKEND_DJANGO:
            return get_state_dj(self)
        elif settings.BACKEND == BACKEND_SQLA:
            return get_state_sqla(self)
        else:
            raise Exception("unknown backend {}".format(settings.BACKEND))
        
    def _linking_as_output(self, dest, link_type):
        """
        :note: Further checks, such as that the output data type is 'Data',
          are done in the super() class.

        :param dest: a Data object instance of the database
        :raise: ValueError if a link from self to dest is not allowed.
        """
        valid_states = [
            calc_states.NEW,
            calc_states.WITHSCHEDULER,
        ]

        if self.get_state() not in valid_states:
            raise ModificationNotAllowed(
                "Can add an output node to a calculation only if it is in one "
                "of the following states: {}, it is instead {}".format(
                    valid_states, self.get_state()))

        return super(Calculation, self)._linking_as_output(dest, link_type)



def run(cs, store=False):
    while True:
        finished = tick_chillstepper(cs, store=store)
        if finished:
            break


def tick_chillstepper(cs, store):

    funcname = cs.get_attr('_next')
    print 1, funcname
    if funcname == 'exit':
        cs._set_state(calc_states.FINISHED)
        return True
    try:
        waiting = QueryBuilder().append(
                ChillstepCalculation, filters={'id':cs.id},tag='parent').append(
                Calculation, filters={'state':{'!in':['FINISHED', 'FAILED']}}, output_of='parent',
                ).count() > 0

        if waiting:
            return False
        returned = getattr(cs, funcname)()
        if returned is None:
            return False
        for k, v in returned.items():
            if isinstance(v, Data):
                v.store()
                v.add_link_from(cs,  label=k, link_type=LinkType.CREATE)
            elif isinstance(v, Calculation):
                print v, v.get_state(), '@' 
                v.add_link_from(cs,  label=k, link_type=LinkType.CALL)
                v.store_all()
                v.submit()
                #~ if not isinstance(v, ChillstepCalculation):
                    #~ raise Exception ("You can only run when only Chillsteppers are involved")
                    #~ run(v)
                
            else:
                raise Exception("Unspecified type {}".format(type(v)))
    except Exception as e:
        msg = "ERROR ! This Chillstepper got an error for {} in the {} method, we report down the stack trace:\n{}".format(
                cs, funcname,traceback.format_exc())
        cs._set_state(calc_states.FAILED)
        cs.add_comment(str(e), user=get_automatic_user())
        print msg
        return True
    



def tick_all():
    qb = QueryBuilder()
    qb.append(ChillstepCalculation, filters={'state':calc_states.WITHSCHEDULER})
    print qb.count()
    for chillstepcalc, in qb.all():
        print "dealing with", chillstepcalc, chillstepcalc.get_attr('_next'),'...'
        tick_chillstepper(chillstepcalc, store=True)



if __name__ == '__main__':
    tick_all()
