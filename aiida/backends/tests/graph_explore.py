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



def _setup_nodes_simple_1():
    from aiida.orm.node import Node
    from aiida.common.links import LinkType
    n1, n2, n3 = [Node().store() for i in range(3)]
    n3.add_link_from(n2, link_type=LinkType.CREATE)
    n2.add_link_from(n1, link_type=LinkType.INPUT)
    return (n1, n2, n3)

def _setup_nodes_cycle():
    from aiida.orm.node import Node
    from aiida.common.links import LinkType
    n1, n2, n3 = [Node().store() for i in range(3)]
    n3.add_link_from(n2, link_type=LinkType.UNSPECIFIED)
    n2.add_link_from(n1, link_type=LinkType.UNSPECIFIED)
    n1.add_link_from(n3, link_type=LinkType.UNSPECIFIED)
    return (n1, n2, n3)

def _setup_nodes_simple_2():
    """
    """
    from aiida.orm.node import Node
    from aiida.common.links import LinkType
    n1, n2, n3, n4 = [Node().store() for i in range(4)]
    n3.add_link_from(n2, link_type=LinkType.CREATE, label='n2_n3')
    n2.add_link_from(n1, link_type=LinkType.INPUT, label='n1_n2')
    n3.add_link_from(n4, link_type=LinkType.CREATE, label='n4_n3')
    n4.add_link_from(n1, link_type=LinkType.INPUT, label='n1_n4')
    return (n1, n2, n3, n4)

@unittest.skipIf(True, '')
class TestRule(AiidaTestCase):


    def test_entity_set(self):
        from aiida.orm.graph import AiidaEntitySet
        from aiida.orm import Node, load_node
        n1,n2,n3 = _setup_nodes_simple_1()
        s =  AiidaEntitySet(Node)
        self.assertFalse(s)
        s.add_keys(n1.pk)
        self.assertTrue(s)
        self.assertEqual(s.get_keys(), set([n1.pk]))
        s.add_aiida_types(n2, n3)
        self.assertEqual(s.get_keys(), set([n1.pk,n2.pk, n3.pk]))

        s2 = AiidaEntitySet(Node, unique_identifier='uuid')

        s2.add_keys(n1.uuid, n2.uuid)
        self.assertEqual(s2.get_keys(), set([n1.uuid, n2.uuid]))

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
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, SubSetOfDB, operators, relationships
        from aiida.orm.node import Node
        n1,n2,n3 = _setup_nodes_simple_1()
        s =  AiidaEntitiesCollection()
        s.nodes.add_aiida_types(n2)
        # Operation 1: A rule to only get the nodes that have input:
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node), 
                operator=operators.ASSIGN, projecting_entity=SubSetOfDB(Node),
                relationship=relationships.LEFTLINKED, relationship_entity=Node
                ).apply(s.copy()).nodes.get_keys(), set([n2.pk]))
        # I do the same instantiating the rule from a string
        self.assertEqual(Operation.get_from_string('N=N<-n').apply(s.copy()).nodes.get_keys(), set([n2.pk]))

        # Operation 2: The same with ancestors:
        # self.assertEqual(to only get the nodes that have input:
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node), 
                operator=operators.ASSIGN, projecting_entity=SubSetOfDB(Node),
                relationship=relationships.LEFTPATH, relationship_entity=Node
            ).apply(s.copy()).nodes.get_keys(), set([n2.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N=N<<n').apply(s.copy()).nodes.get_keys(), set([n2.pk]))


        # A rule that assignes the outputs of N to N
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node), 
                operator=operators.ASSIGN, projecting_entity=Node,
                relationship=relationships.LEFTLINKED, relationship_entity=SubSetOfDB(Node)
            ).apply(s.copy()).nodes.get_keys(), set([n3.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N=n<-N').apply(s.copy()).nodes.get_keys(), set([n3.pk]))


        # A rule that assignes the ancestors of N to N
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node), 
                operator=operators.ASSIGN, projecting_entity=Node,
                relationship=relationships.LEFTPATH, relationship_entity=SubSetOfDB(Node)
            ).apply(s.copy()).nodes.get_keys(), set([n3.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N=n<<N').apply(s.copy()).nodes.get_keys(), set([n3.pk]))


        # A rule that assignes the inputs of N to N
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node), 
                operator=operators.ASSIGN, projecting_entity=Node,
                relationship=relationships.RIGHTLINKED, relationship_entity=SubSetOfDB(Node)
            ).apply(s.copy()).nodes.get_keys(), set([n1.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N=n->N').apply(s.copy()).nodes.get_keys(), set([n1.pk]))


        # A rule that updates N with the inputs of N
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node), 
                operator=operators.UPDATE, projecting_entity=Node,
                relationship=relationships.RIGHTLINKED, relationship_entity=SubSetOfDB(Node)
            ).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N+=n->N').apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk]))


        # A rule that reassigns N with inputs from N
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node), 
                operator=operators.ASSIGN, projecting_entity=Node,
                relationship=relationships.LINKED, relationship_entity=SubSetOfDB(Node)
            ).apply(s.copy()).nodes.get_keys(), set([n1.pk, n3.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N=n--N').apply(s.copy()).nodes.get_keys(), set([n1.pk, n3.pk]))


        # I create a new collection
        s =  AiidaEntitiesCollection()
        s.nodes.add_aiida_types(n1, n2)

        # A rule that removes N with inputs from N
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node),
                operator=operators.REMOVE, projecting_entity=SubSetOfDB(Node),
                relationship=relationships.LEFTLINKED, relationship_entity=Node
            ).apply(s.copy()).nodes.get_keys(), set([n1.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N-=N<-n').apply(s.copy()).nodes.get_keys(), set([n1.pk]))

        # A rule that assigns N to all descendants of N:
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node),
                operator=operators.ASSIGN, projecting_entity=Node,
                relationship=relationships.LEFTPATH, relationship_entity=SubSetOfDB(Node)
            ).apply(s.copy()).nodes.get_keys(), set([n2.pk, n3.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N=n<<N').apply(s.copy()).nodes.get_keys(), set([n2.pk, n3.pk]))

        # A rule that assigns N to all ancestors  of N
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node),
                operator=operators.ASSIGN, projecting_entity=Node,
                relationship=relationships.RIGHTPATH, relationship_entity=SubSetOfDB(Node)
            ).apply(s.copy()).nodes.get_keys(), set([n1.pk]))
        # Again, trying with string:
        self.assertEqual(Operation.get_from_string('N=n>>N').apply(s.copy()).nodes.get_keys(), set([n1.pk]))


    def test_node_group_relationship(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, SubSetOfDB, operators, relationships
        from aiida.orm import Node, Group
        n1,n2,n3 = _setup_nodes_simple_1()
        g1 = Group(name='test1').store()
        g2 = Group(name='test2').store()
        g3 = Group(name='test3').store()
        g1.add_nodes((n1,n2))
        g2.add_nodes((n2,n3))
        s =  AiidaEntitiesCollection()
        # s now contains g1 as a starting point!
        s.groups.add_aiida_types(g1)
        # This rule asserts whether I can find all the nodes that belong to the groups in the set:
        self.assertEqual(Operation(set_entity=SubSetOfDB(Node),
                operator=operators.ASSIGN, projecting_entity=Node,
                relationship=relationships.LINKED, relationship_entity=SubSetOfDB(Group)
            ).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk]))
        # Same with a string:
        self.assertEqual(Operation.get_from_string('N=n--G').apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk]))
        # The collection now contains g1 as well as n3
        s.nodes.add_aiida_types(n3)
        # This rule overwrites whatever was set, so it should only return n1 and n2
        self.assertEqual(Operation.get_from_string('N=n--G').apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk]))
        # This rule updates the node set, so it should also n3
        self.assertEqual(Operation.get_from_string('N+=n--G').apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk]))
        # This rule subtracts, so only n3 remains
        self.assertEqual(Operation.get_from_string('N-=n--G').apply(s.copy()).nodes.get_keys(), set([n3.pk]))


        # Now I'm looking at the reverse process, seeing whether I can get groups from the starting nodes:
        s1 =  AiidaEntitiesCollection()
        s1.nodes.add_aiida_types(n1)
        s2 =  AiidaEntitiesCollection()
        s2.nodes.add_aiida_types(n2)
        s3 =  AiidaEntitiesCollection()
        s3.nodes.add_aiida_types(n3)
        s4 =  AiidaEntitiesCollection()

        # I see whether I get all the groups that n1 belongs to:
        self.assertEqual(Operation.get_from_string('G=g--N').apply(s1.copy()).groups.get_keys(), set([g1.pk]))
        # I see whether I get all the groups that n2 belongs to:
        self.assertEqual(Operation.get_from_string('G=g--N').apply(s2.copy()).groups.get_keys(), set([g1.pk, g2.pk]))
        # I see whether I get all the groups that n3 belongs to:
        self.assertEqual(Operation.get_from_string('G=g--N').apply(s3.copy()).groups.get_keys(), set([g2.pk]))

        # Can I get all the groups that are connected to nodes:
        self.assertEqual(Operation.get_from_string('G=g--n').apply(s4.copy()).groups.get_keys(), set([g1.pk, g2.pk]))
        # Can I get all the groups:
        self.assertEqual(Operation.get_from_string('G=g').apply(s4.copy()).groups.get_keys(), set([g1.pk, g2.pk, g3.pk]))

    def test_node_computer_relationship(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation
        from aiida.orm import Node, Computer
        n1,n2,n3 = _setup_nodes_simple_1()
        c = Computer(name='aaa',
                            hostname='aaa',
                            transport_type='local',
                            scheduler_type='pbspro',
                            workdir='/tmp/aiida')
        c.store()
        n4 = Node(computer=c).store()

        s =  AiidaEntitiesCollection()
        s.computers.add_aiida_types(c)
        self.assertEqual(Operation.get_from_string('N=n--C').apply(s.copy()).nodes.get_keys(), set([n4.pk]))
        s.nodes.add_aiida_types(n1)
        self.assertEqual(Operation.get_from_string('N=n--C').apply(s.copy()).nodes.get_keys(), set([n4.pk]))
        self.assertEqual(Operation.get_from_string('N+=n--C').apply(s.copy()).nodes.get_keys(), set([n1.pk, n4.pk]))

        self.assertTrue(c.id not in  Operation.get_from_string('C=c--N').apply(s.copy()).computers.get_keys())
        s.nodes.add_aiida_types(n4)
        self.assertTrue(c.id in Operation.get_from_string('C=c--N').apply(s.copy()).computers.get_keys())

    def test_node_user_relationship(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation
        from aiida.orm import User, Node
        from aiida.backends.utils import get_automatic_user

        # These nodes are set up with automatic user!
        n1,n2,n3 = _setup_nodes_simple_1()


        user = User(email="newuser@new.n")
        user.force_save()

        n4 = Node()
        n4.dbnode.user = user._dbuser
        n4.store()

        s =  AiidaEntitiesCollection()
        s.users.add_aiida_types(user)
        self.assertEqual(Operation.get_from_string('N=n--U').apply(s.copy()).nodes.get_keys(), set([n4.pk]))
        # let's see the automatic user!
        automatic_user = get_automatic_user().get_aiida_class()
        s =  AiidaEntitiesCollection()
        s.users.add_aiida_types(automatic_user)
        s.nodes.add_aiida_types(n1, n2, n3, n4)
        self.assertEqual(Operation.get_from_string('N-=n--U').apply(s.copy()).nodes.get_keys(), set([n4.pk]))
        self.assertEqual(Operation.get_from_string('U=u--N').apply(s.copy()).users.get_keys(), set([automatic_user.id, user.id]))

        s =  AiidaEntitiesCollection()
        s.users.add_aiida_types(user)
        s.nodes.add_aiida_types(n1)
        self.assertEqual(Operation.get_from_string('N+=n--U').apply(s.copy()).nodes.get_keys(), set([n1.pk, n4.pk]))
        self.assertEqual(Operation.get_from_string('N=n--U').apply(s.copy()).nodes.get_keys(), set([n4.pk]))
        self.assertEqual(Operation.get_from_string('U=u--N').apply(s.copy()).users.get_keys(), set([automatic_user.id]))

#~ @unittest.skipIf(True, '')
class TestRuleSequence(AiidaTestCase):

    def test_rule_sequence_simple(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence
        n1,n2,n3, n4 = _setup_nodes_simple_2()

        s =  AiidaEntitiesCollection()
        s.nodes.add_keys(n2.pk)
        # Operation gets the inputs
        r1 = Operation.get_from_string('N=n->N')
        # Operation gets the outputs
        r2 = Operation.get_from_string('N=n<-N')
        # Operation that updates with the inputs
        r3 = Operation.get_from_string('N+=n->N')
        # Operation that updates with outputs
        r4 = Operation.get_from_string('N+=n<-N')

        self.assertEqual(r1.apply(s.copy()).nodes.get_keys(), set([n1.pk]))
        self.assertEqual(r2.apply(s.copy()).nodes.get_keys(), set([n3.pk]))

        self.assertEqual(RuleSequence(r1,r2).apply(s.copy()).nodes.get_keys(), set([n2.pk, n4.pk]))
        self.assertEqual(RuleSequence(r2, r1).apply(s.copy()).nodes.get_keys(), set([n2.pk, n4.pk]))
        # Now I first update with inputs and then with outputs
        self.assertEqual(RuleSequence(r3, r4).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk, n4.pk]))
        # Now I first update with outputs and then with inputs
        self.assertEqual(RuleSequence(r4, r3).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk, n4.pk]))


    def test_iterations_node_1(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence
        n1,n2,n3,n4 = _setup_nodes_simple_2()
        r2 = Operation.get_from_string('N=n<-N')
        s =  AiidaEntitiesCollection()
        s.nodes.set_keys(n1.pk)
        # This get's the direct outputs of n1
        self.assertEqual(RuleSequence(r2, niter=1).apply(s.copy()).nodes.get_keys(), set([n2.pk, n4.pk]))
        # This get's the outputs of the outputs of n1
        self.assertEqual(RuleSequence(r2, niter=2).apply(s.copy()).nodes.get_keys(), set([n3.pk]))
        # This get's the outputs of the outputs of the outputs of n1
        self.assertEqual(RuleSequence(r2, niter=3).apply(s.copy()).nodes.get_keys(), set([]))

    def test_iterations_cycle(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence
        nodes = _setup_nodes_cycle()
        r1 = Operation.get_from_string('N=n<-N')
        r2 = Operation.get_from_string('N+=n<-N')
        s =  AiidaEntitiesCollection()
        s.nodes.set_keys(nodes[0].pk)
        rs1 = RuleSequence(r1)
        rs2 = RuleSequence(r2)
        # This follows the cycle for a fixed number of iterations:
        for i in range(1, 10):
            rs1.set_niter(i)
            self.assertEqual(rs1.apply(s.copy()).nodes.get_keys(), set([nodes[i%3].pk]))
        pks = [n.pk for n in nodes]
        # The update rule should not go bananas here, and stop  after three iterations:
        for i in range(1, 10):
            rs2.set_niter(i)
            self.assertEqual(rs2.apply(s.copy()).nodes.get_keys(), set(pks[:i+1]))
            self.assertEqual(rs2.get_last_niter(),min((i, 3)))


    def test_iterations_node_2(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence
        n1,n2,n3,n4 = _setup_nodes_simple_2()
        r2 = Operation.get_from_string('N+=n<-N')
        s =  AiidaEntitiesCollection()
        s.nodes.set_keys(n1.pk)
        # This get's the direct outputs of n1
        rs = RuleSequence(r2, niter=1)
        self.assertEqual(rs.apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n4.pk]))
        self.assertEqual(1, rs.get_last_niter())
        rs.set_niter(2)
        # This get's the outputs of the outputs of n1
        self.assertEqual(rs.apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk, n4.pk]))
        self.assertEqual(2, rs.get_last_niter())

        # This get's the outputs of the outputs of the outputs of n1
        rs.set_niter(10)
        self.assertEqual(rs.apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk, n4.pk]))
        # I'm now checking that he's not doing more iterations than necessary (i.e. the tricks!)
        self.assertEqual(3, rs.get_last_niter())


    def test_iter_check_cycle(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence

        n1,n2,n3 = _setup_nodes_cycle()
        # This rule updates with the outputs:
        r = Operation.get_from_string('N+=n<-N')
        s =  AiidaEntitiesCollection()
        s.nodes.add_keys(n1.pk)
        self.assertEqual(RuleSequence(r, niter=1).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk]))
        self.assertEqual(RuleSequence.get_from_string('(N+=n<N)2').apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk]))
        

        self.assertEqual(RuleSequence(r, niter=2).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk]))
        self.assertEqual(RuleSequence(r, niter=3).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk]))
        self.assertEqual(RuleSequence(r, niter=4).apply(s.copy()).nodes.get_keys(), set([n1.pk, n2.pk, n3.pk]))


    def test_rule_sequence_from_string(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence
        n1,n2,n3 = _setup_nodes_cycle()
        r =RuleSequence.get_from_string('(N+=n<N)2')

@unittest.skipIf(True, '')
class TestStashCommit(AiidaTestCase):
    def test_stash(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence, StashCommit, StashPop
        n1,n2,n3,n4 = _setup_nodes_simple_2()
        #~ print
        #~ print [n.pk for n in (n1,n2,n3,n4)]
        # Operation to update with outputs
        rout = Operation.get_from_string('N+=n<<N')
        # Operation to update with inputs
        rinp = Operation.get_from_string('N+=n>>N')
        s =  AiidaEntitiesCollection()
        s.nodes.add_aiida_types(n2)

        self.assertEqual(
            RuleSequence(StashCommit(), rinp, StashPop(), rout).apply(s.copy()).nodes.get_keys(),
            set([n1.pk, n2.pk, n3.pk]))
        # reversing the rules should lead to the same outcome
        self.assertEqual(
            RuleSequence(StashCommit(), rout, StashPop(), rinp).apply(s.copy()).nodes.get_keys(),
            set([n1.pk, n2.pk, n3.pk]))
        # The outcome is of course different without the stash usage!
        self.assertEqual(
            RuleSequence(rout, rinp).apply(s.copy()).nodes.get_keys(),
            set([n1.pk, n2.pk, n3.pk, n4.pk])
        )

@unittest.skipIf(True, '')
class TestLinksRules(AiidaTestCase):
    def test_links(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence, StashCommit, StashPop
        n1,n2,n3,n4 = _setup_nodes_simple_2()
        s =  AiidaEntitiesCollection()
        s.nodes.add_keys(n2.pk)

        r1 = Operation.get_from_string('N+=n->N')
        
        r2 = Operation.get_from_string('N+=n<-N')

        values = RuleSequence(r1,r2).apply(s.copy()).nodes_nodes.values()
        vshould = set([
            (n1.pk, n2.pk, u'n1_n2', u'inputlink'),
            (n1.pk, n4.pk, u'n1_n4', u'inputlink'), 
            (n2.pk, n3.pk, u'n2_n3', u'createlink')])
        #~ print '@@@@@@@@@@@@@@', len(values)
        self.assertEqual(set(values), vshould)

        values = RuleSequence(r1,r2, r1).apply(s.copy()).nodes_nodes.values()
        vshould = set([
            (n1.pk, n2.pk, u'n1_n2', u'inputlink'),
            (n1.pk, n4.pk, u'n1_n4', u'inputlink'), 
            (n2.pk, n3.pk, u'n2_n3', u'createlink'),
            (n4.pk, n3.pk, u'n4_n3', u'createlink')])
        self.assertEqual(set(values), vshould)


    def test_group_nodes(self):
        from aiida.orm.graph import AiidaEntitiesCollection, Operation, RuleSequence, StashCommit, StashPop
        from aiida.orm import Group
        n1,n2,n3 = _setup_nodes_simple_1()
        g1 = Group(name='a').store()
        g2 = Group(name='b').store()

        g1.add_nodes((n1, n3))
        g2.add_nodes((n2,n3))

        s =  AiidaEntitiesCollection()
        s.nodes.add_keys(n2.pk)

        r1 = Operation.get_from_string('G+=g--N')
        
        r2 = Operation.get_from_string('N+=n--G')


        res = RuleSequence(r1).apply(s.copy())
        self.assertFalse(res.nodes_nodes.values())
        #~ print res.nodes_groups.values()
        self.assertEqual(set(res.nodes_groups.values()), set([(n2.pk, g2.id)]))

        res = RuleSequence(r1, r2).apply(s.copy())
        self.assertFalse(res.nodes_nodes.values())
        self.assertEqual(set(res.nodes_groups.values()), set([(n3.pk,g2.id), (n2.pk, g2.id)]))

        res = RuleSequence(r1, r2, r1).apply(s.copy())
        self.assertFalse(res.nodes_nodes.values())
        self.assertEqual(
            set(res.nodes_groups.values()),
            set([(n3.pk,g2.id), (n3.pk,g1.id), (n2.pk, g2.id)])
        )

        res = RuleSequence(r1, r2, r1, r2).apply(s.copy())
        self.assertFalse(res.nodes_nodes.values())
        self.assertEqual(
            set(res.nodes_groups.values()),
            set([(n3.pk,g2.id), (n3.pk,g1.id),(n1.pk,g1.id), (n2.pk, g2.id)])
        )

        res = RuleSequence(r1, r2, niter=2).apply(s.copy())
        self.assertFalse(res.nodes_nodes.values())
        self.assertEqual(
            set(res.nodes_groups.values()),
            set([(n3.pk,g2.id), (n3.pk,g1.id),(n1.pk,g1.id), (n2.pk, g2.id)])
        )

