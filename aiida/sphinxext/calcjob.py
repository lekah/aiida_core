# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida-core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################
"""
Defines an rst directive to auto-document AiiDA calculation job.
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from .process import AiidaProcessDocumenter, AiidaProcessDirective


def setup_extension(app):
    app.add_directive_to_domain('py', AiidaCalcJobDocumenter.directivetype, AiidaCalcJobDirective)
    app.add_autodocumenter(AiidaCalcJobDocumenter)


class AiidaCalcJobDocumenter(AiidaProcessDocumenter):
    """Sphinx Documenter for AiiDA CalcJobs."""
    directivetype = 'aiida-calcjob'
    objtype = 'calcjob'
    priority = 20

    @classmethod
    def can_document_member(cls, member, membername, isattr, parent):
        from aiida.engine import CalcJob
        return issubclass(cls, CalcJob)


class AiidaCalcJobDirective(AiidaProcessDirective):
    signature = 'CalcJob'
    annotation = 'calcjob'
