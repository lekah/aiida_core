# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida_core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
import os
import sys

from aiida.backends.utils import load_dbenv, is_dbenv_loaded
from aiida.cmdline import delayed_load_node as load_node
from aiida.cmdline.baseclass import VerdiCommandWithSubcommands


class Chillstep(VerdiCommandWithSubcommands):
    """
    Query and interact with calculations

    Different subcommands allow to list the running calculations, show the
    content of the input/output files, see the logs, etc.
    """

    def __init__(self):
        """
        A dictionary with valid commands and functions to be called:
        list.
        """
        from aiida.cmdline.commands.node import _Label, _Description

        labeler = _Label('calculation')
        descriptioner = _Description('calculation')

        self.valid_subcommands = {
            #~ 'gotocomputer': (self.calculation_gotocomputer, self.complete_none),
            'list': (self.chillstep_list, self.complete_none),
            #~ 'logshow': (self.calculation_logshow, self.complete_none),
            #~ 'kill': (self.calculation_kill, self.complete_none),
            #~ 'inputls': (self.calculation_inputls, self.complete_none),
            #~ 'outputls': (self.calculation_outputls, self.complete_none),
            #~ 'inputcat': (self.calculation_inputcat, self.complete_none),
            #~ 'outputcat': (self.calculation_outputcat, self.complete_none),
            #~ 'res': (self.calculation_res, self.complete_none),
            #~ 'show': (self.calculation_show, self.complete_none),
            #~ 'plugins': (self.calculation_plugins, self.complete_plugins),
            #~ 'cleanworkdir': (self.calculation_cleanworkdir, self.complete_none),
            #~ 'label': (labeler.run, self.complete_none),
            #~ 'description': (descriptioner.run, self.complete_none),
        }


    def chillstep_list(self, *args):
        """
        Return a list of calculations on screen.
        """

        if not is_dbenv_loaded():
            load_dbenv()


        from aiida.orm.querybuilder import QueryBuilder
        from aiida.orm.calculation.chillstep import ChillstepCalculation
        
        res = QueryBuilder().append(ChillstepCalculation).all()
        for chiller, in res:
            print chiller.label, chiller.id, chiller.get_state()
        return
        parser = argparse.ArgumentParser(
            prog=self.get_full_command_name(),
            description='List AiiDA calculations.')
        # The default states are those that are shown if no option is given
        parser.add_argument('-s', '--states', nargs='+', type=str,
                            help="show only the AiiDA calculations with given state",
                            default=[calc_states.WITHSCHEDULER,
                                     calc_states.NEW,
                                     calc_states.TOSUBMIT,
                                     calc_states.SUBMITTING,
                                     calc_states.COMPUTED,
                                     calc_states.RETRIEVING,
                                     calc_states.PARSING,
                                     ])

        parser.add_argument('-p', '--past-days', metavar='N',
                            help="add a filter to show only calculations created in the past N days",
                            action='store', type=int)
        parser.add_argument('-g', '--group', '--group-name',
                            metavar='GROUPNAME',
                            help="add a filter to show only calculations within a given group",
                            action='store', type=str)
        parser.add_argument('-G', '--group-pk', metavar='GROUPPK',
                            help="add a filter to show only calculations within a given group",
                            action='store', type=int)
        parser.add_argument('pks', type=int, nargs='*',
                            help="a list of calculations to show. If empty, all running calculations are shown. If non-empty, ignores the -p and -r options.")
        parser.add_argument('-a', '--all-states',
                            dest='all_states', action='store_true',
                            help="Overwrite manual set of states if present, and look for calculations in every possible state")
        parser.set_defaults(all_states=False)
        parser.add_argument('-A', '--all-users',
                            dest='all_users', action='store_true',
                            help="Show calculations for all users, rather than only for the current user")
        parser.set_defaults(all_users=False)
        parser.add_argument('-t', '--absolute-time',
                            dest='relative_ctime', action='store_false', default=True,
                            help="Print the absolute creation time, rather than the relative creation time")
        parser.add_argument('-l', '--limit',
                            type=int, default=None,
                            help='set a limit to the number of rows returned')
        parser.add_argument('-o', '--order-by',
                            choices=['id', 'ctime'],
                            default='ctime',
                            help='order the results')
        parser.add_argument('--project',
                            choices=(
                                    'pk', 'state', 'ctime', 'sched', 'computer',
                                    'type', 'description', 'label', 'uuid',
                                    'mtime', 'user'
                                ),
                            nargs='+',
                            default=('pk', 'state', 'ctime', 'sched', 'computer', 'type'),
                            help="Define the list of properties to show"
                        )

        args = list(args)
        parsed_args = parser.parse_args(args)

        capital_states = [i.upper() for i in parsed_args.states]
        parsed_args.states = capital_states

        if parsed_args.all_states:
            parsed_args.states = None

        C._list_calculations(
            states=parsed_args.states,
            past_days=parsed_args.past_days,
            pks=parsed_args.pks,
            all_users=parsed_args.all_users,
            group=parsed_args.group,
            group_pk=parsed_args.group_pk,
            relative_ctime=parsed_args.relative_ctime,
            # with_scheduler_state=parsed_args.with_scheduler_state,
            order_by=parsed_args.order_by,
            limit=parsed_args.limit,
            projections=parsed_args.project,
        )

