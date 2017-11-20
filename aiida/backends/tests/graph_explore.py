# -*- coding: utf-8 -*-
###########################################################################
# Copyright (c), The AiiDA team. All rights reserved.                     #
# This file is part of the AiiDA code.                                    #
#                                                                         #
# The code is hosted on GitHub at https://github.com/aiidateam/aiida_core #
# For further information on the license, see the LICENSE.txt file        #
# For further information please visit http://www.aiida.net               #
###########################################################################

from aiida.backends.testbase import AiidaTestCase
import unittest
import aiida.backends.settings as settings


class TestGraphExplorer(AiidaTestCase):
    def _setup_nodes_simple_1(self):
        from aiida.orm.node import Node
        n1, n2, n3 = [Node().store() for i in range(3)]
        n3.add_link_from(n2)
        n2.add_link_from(n1)
        return (n1, n2, n3)


    def test_entity_set(self):
        from aiida.orm.graph import AiidaEntitySet
        from aiida.orm.node import Node
        n1,n2,n3 = self._setup_nodes_simple_1()
        s =  AiidaEntitySet(Node)
        s.add(n1)
        self.assertEqual(s.set, set([n1.pk]))
        s.add(n2.pk, n3)
        self.assertEqual(s.set, set([n1.pk, n2.pk, n3.pk]))
        
        s2 = AiidaEntitySet(Node, unique_identifier='uuid')
        s2.add(n1, n2)
        self.assertEqual(s2.set, set([n1.uuid, n2.uuid]))

    def test_regex(self):
        from aiida.orm.graph import RULE_REGEX
        for valid_match in ('N=N<-n', 'N+=n', 'B-=B--g'):
            match = RULE_REGEX.search(valid_match)
            self.assertTrue(match is not None)
        for non_valid_match in ('N=N<n', 'N+n', 'B-=B-g', # not valid because of missing oprerators
                ' N+=n', 'N+=n ', # start or end with space
            ):
            match = RULE_REGEX.search(non_valid_match)
            self.assertTrue(match is None)


    def test_simple_node_relationship(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Rule
        from aiida.orm.node import Node
        n1,n2,n3 = self._setup_nodes_simple_1()
        s =  AiidaEntitiesCollection()
        s.nodes.add(n2)
        # A rule to only get the nodes that have input!
        self.assertEqual(Rule.get_from_string('N=N<-n').apply(s.copy()).nodes.set, set([n2.pk]))
        # A rule that assignes the outputs of N to N
        self.assertEqual(Rule.get_from_string('N=n<-N').apply(s.copy()).nodes.set, set([n3.pk]))
        # A rule that assignes the inputs of N to N
        self.assertEqual(Rule.get_from_string('N=n->N').apply(s.copy()).nodes.set, set([n1.pk]))
        # A rule that updates N with the inputs of N
        self.assertEqual(Rule.get_from_string('N+=n->N').apply(s.copy()).nodes.set, set([n1.pk, n2.pk]))
        # A rule that removes N with inputs from N
        s =  AiidaEntitiesCollection()
        s.nodes.add(n1, n2)
        self.assertEqual(Rule.get_from_string('N-=N<-n').apply(s.copy()).nodes.set, set([n1.pk]))

