from aiida.orm.calculation.chillstep import ChillstepCalculation
from aiida.orm.data.parameter import ParameterData
from aiida.orm import Data, load_node, Calculation
#~ from aiida.orm.data.structure import StructureData
from aiida.orm.data.array.trajectory import TrajectoryData
from aiida.orm.querybuilder import QueryBuilder
from aiida.orm.calculation.inline import InlineCalculation, optional_inline
from aiida.common.datastructures import calc_states
from aiida.common.links import LinkType

import numpy as np

SINGULAR_TRAJ_KEYS = ('symbols','atomic_species_name')

@optional_inline
def concatenate_trajectory_inline(**kwargs):
    for k, v in kwargs.iteritems():
        if not isinstance(v, TrajectoryData):
            raise Exception("All my inputs have to be instances of TrajectoryData")
    sorted_trajectories = zip(*sorted(kwargs.items()))[1]

    # I assume they store the same arrays!
    arraynames = sorted_trajectories[0].get_arraynames()
    traj = TrajectoryData()
    for arrname in arraynames:
        if arrname in SINGULAR_TRAJ_KEYS:
            traj.set_array(arrname, sorted_trajectories[0].get_array(arrname))
        else:
            traj.set_array(arrname, np.concatenate([t.get_array(arrname) for t in sorted_trajectories]))
    
    
    [traj._set_attr(k,v) for k,v in sorted_trajectories[0].get_attrs().items() if not k.startswith('array|')]
    #~ for k in keys:


    return {'concatenated_trajectory':traj}
    

def get_completed_number_of_steps(calc):
    try:
        nstep = calc.res.nstep
    except AttributeError:
        nstep = calc.out.output_trajectory.get_attr('array|positions.0')
    return nstep



class MoldynCalculation(ChillstepCalculation):
    """
    Run a Molecular Dynamics calculations
    """
    _MAX_ITERATIONS = 999
    def start(self):
        print "starting"
        self.ctx.steps_todo = self.inputs.moldyn_parameters.dict.nstep # Number of steps I have to do:
        self.ctx.max_steps_percalc = self.inputs.moldyn_parameters.dict.max_steps_percalc # Number of steps I have to do:
        self.ctx.steps_done = 0 # Number of steps done, obviously 0 in the beginning
        self.goto(self.firstcalc)
        self.ctx.iteration = 0

    def firstcalc(self):
        # I simply us all my inputs here as input for the calculation!
        calc = self.inp.code.new_calc()
        for linkname, input_node in self.get_inputs_dict().iteritems():
            if linkname.startswith('moldyn_'): # stuff only for the moldyn workflow has this prefix!
                continue
            if isinstance(input_node, Data):
                calc.add_link_from(input_node, label=linkname)
        # set the resources:
        input_dict = self.inp.parameters.get_dict()
        input_dict['CONTROL']['nstep'] = min((self.ctx.steps_todo, self.ctx.max_steps_percalc))
        calc.use_parameters(ParameterData(dict=input_dict))
        calc.set_resources(self.inputs.moldyn_parameters.dict.resources)
        calc.set_max_wallclock_seconds(self.inputs.moldyn_parameters.dict.max_wallclock_seconds)
        self.ctx.lastcalc_uuid = calc.uuid
        self.goto(self.iterate)
        return {'calc_{}'.format(str(self.ctx.iteration).rjust(len(str(self._MAX_ITERATIONS)),str(0))):calc}

    
    def iterate(self):
        """
        Check if I have to run again. If not, I exit
        """
        lastcalc = load_node(self.ctx.lastcalc_uuid)
        if lastcalc.get_state() != calc_states.FINISHED:
            raise Exception("My last calculation {} did not finish".format(lastcalc))
        # get the steps I run
        nsteps_run_last_calc = get_completed_number_of_steps(lastcalc)
        self.ctx.steps_todo -= nsteps_run_last_calc
        self.ctx.steps_done += nsteps_run_last_calc
        if self.ctx.steps_todo > 0:
            # I have to run another calculation
            self.ctx.iteration += 1
            newcalc = lastcalc.create_restart(parent_folder_symlink=True) # I don't really care how this is achieved, the plugin needs to create a valid restart!
            input_dict = newcalc.inp.parameters.get_dict()
            input_dict['CONTROL']['nstep'] = min((self.ctx.steps_todo, self.ctx.max_steps_percalc))
            newcalc.use_parameters(ParameterData(dict=input_dict))
            self.goto(self.iterate)
            self.ctx.lastcalc_uuid = newcalc.uuid
            return {'calc_{}'.format(str(self.ctx.iteration).rjust(len(str(self._MAX_ITERATIONS)),str(0))):newcalc}
        else:
            self.goto(self.exit)# I finish


    def get_slave_calculations(self):
        """
        Returns a list of the calculations that was called by the WF, ordered.
        """
        qb = QueryBuilder()
        qb.append(MoldynCalculation, filters={'id':self.id}, tag='m')
        qb.append(Calculation, output_of='m', edge_project='label', edge_filters={'type':LinkType.CALL.value, 'label':{'like':'calc_%'}}, tag='c', edge_tag='mc', project='*')
        d = {item['mc']['label']:item['c']['*'] for item in qb.iterdict()}
        sorted_calcs = sorted(d.items())
        return zip(*sorted_calcs)[1]

    def get_output_trajectory(self, store=False):
        # I don't even have to be finished,  for this
        qb = QueryBuilder()
        qb.append(MoldynCalculation, filters={'id':self.id}, tag='m')
        qb.append(Calculation, output_of='m', edge_project='label', edge_filters={'type':LinkType.CALL.value, 'label':{'like':'calc_%'}}, tag='c', edge_tag='mc')
        qb.append(TrajectoryData, output_of='c', project='*', tag='t')
        d = {item['mc']['label'].replace('calc_', 'trajectory_'):item['t']['*'] for item in qb.iterdict()}
        return concatenate_trajectory_inline(store=store, **d)['concatenated_trajectory']

        
        
        
        
        
