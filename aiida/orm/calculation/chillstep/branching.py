from aiida.orm.calculation.chillstep import ChillstepCalculation
from moldyn import MoldynCalculation
from aiida.orm.data.parameter import ParameterData
from aiida.orm.data.structure import StructureData
from aiida.orm import Data, load_node
from aiida.orm.calculation.inline import optional_inline
from aiida.common.constants import bohr_to_ang
from aiida.orm.querybuilder import QueryBuilder
from aiida.common.links import LinkType


import numpy as np

@optional_inline
def get_structure_from_trajectory_for_pinball_inline(structure, trajectory, settings, parameters):
    step_index = parameters.dict.step_index
    pos_units = parameters.dict.pos_units
    vel_units = parameters.dict.vel_units
    recenter = parameters.get_dict().get('recenter', False)
    assert pos_units == 'atomic',""
    assert vel_units == 'atomic',""
    atoms = trajectory.get_step_structure(step_index).get_ase()
    velocities = trajectory.get_step_data(step_index)[-1]
    
    if recenter:
        com = np.zeros(3)
        M =0.
        # Calculate the center of mass displacement:
        for atom,vel in zip(atoms, velocities):
            com = com + atom.mass*vel
            M += atom.mass
        #~ print vel, 1000*atom.mass*vel, com
        velocities[:,0:3] -= com[0:3]/M
        # CHECK:
        com = np.zeros(3)
        for atom,vel in zip(atoms, velocities):
            com = com + atom.mass*vel
        assert abs(np.linalg.norm(com)) < 1e-12, "COM did not disappear"
    velocities = velocities.tolist()
    for atom in atoms:
        atom.position *= bohr_to_ang
    for atom in structure.get_ase()[len(atoms):]:
        atoms.append(atom)
        velocities.append([0.,0.,0.])
    newstruc = StructureData(ase=atoms)
    newstruc.label = structure.label  # Transferring the label, because why not

    settings_d = settings.get_dict()
    settings_d['ATOMIC_VELOCITIES'] = velocities

    return dict(
            structure=newstruc,
            settings=ParameterData(dict=settings_d)
        )

class BranchingCalculation(ChillstepCalculation):
    """
    Run a Molecular Dynamics calculations
    """
    def start(self):
        print "starting"
        # Get the parameters
        self.ctx.nr_of_branches = self.inputs.parameters_branching.dict.nr_of_branches
        self.goto(self.thermalize)
        assert self.inp.parameters_NVT.dict['IONS']['ion_velocities'] == 'from_input'
        assert self.inp.parameters_NVE.dict['IONS']['ion_velocities'] == 'from_input'


    def thermalize(self):
        """
        Thermalize a run! This is the first set of calculations, I thermalize with the criterion
        being the number of steps set in moldyn_parameters_thermalize.dict.nstep
        """
        # all the settings are the same for thermalization, NVE and NVT
        inp_d = {k:v for k,v in self.get_inputs_dict().items() if not 'parameters_' in k}
        inp_d['moldyn_parameters'] = self.inp.moldyn_parameters_thermalize
        inp_d['parameters'] = self.inp.parameters_thermalize
        self.goto(self.run_NVT)
        return {'thermalizer':MoldynCalculation(**inp_d)}

    def run_NVT(self):
        """
        Here I restart from the the thermalized run! I run NVT until I have reached the
        number of steps specified in self.inp.moldyn_parameters_NVT.dict.nstep
        """
        inp_d = {k:v for k,v in self.get_inputs_dict().items() if not 'parameters_' in k}
        inp_d['moldyn_parameters'] = self.inp.moldyn_parameters_NVT
        inp_d['parameters'] = self.inp.parameters_NVT
        

        traj = self.out.thermalizer.get_output_trajectory(store=True)

        try:
            settings = self.inp.settings
        except:
            settings = ParameterData().store()
        slaves = {}
        res = get_structure_from_trajectory_for_pinball_inline(
                structure=self.inp.structure, trajectory=traj,
                settings=settings,
                parameters=ParameterData(dict=dict(
                        step_index=-1,
                        pos_units=traj.get_attr('units|positions'),
                        vel_units=traj.get_attr('units|velocities'),
                        recenter=True
            )), store=True)
        inp_d['settings']=res['settings']
        inp_d['structure']=res['structure']

        self.goto(self.run_NVE)
        return {'slave_NVT':MoldynCalculation(**inp_d)}

    def run_NVE(self):
        inp_d = {k:v for k,v in self.get_inputs_dict().items() if not 'parameters_' in k}
        inp_d['moldyn_parameters'] = self.inp.moldyn_parameters_NVE
        inp_d['parameters'] = self.inp.parameters_NVE

        traj = self.out.slave_NVT.get_output_trajectory(store=True)

        trajlen = traj.get_positions().shape[0]
        block_length =  1.0*trajlen / self.ctx.nr_of_branches
        
        indices = [int(i*block_length)-1 for i in range(1, self.ctx.nr_of_branches+1)]
        try:
            settings = self.inp.settings
        except:
            settings = ParameterData().store()
        slaves = {}
        for count, idx in enumerate(indices):
            print count
            res = get_structure_from_trajectory_for_pinball_inline(
                    structure=self.inp.structure, trajectory=traj,
                    settings=settings,
                    parameters=ParameterData(dict=dict(
                            step_index=idx,
                            pos_units=traj.get_attr('units|positions'),
                            vel_units=traj.get_attr('units|velocities'),
                            recenter=True
                    )), store=True)
            inp_d['settings']=res['settings']
            inp_d['structure']=res['structure']
            slaves['slave_NVE_{}'.format(str(count).rjust(len(str(len(indices))),str(0)))] = MoldynCalculation(**inp_d)
        self.goto(self.exit)
        return slaves

    def get_output_trajectories(self, store=False):
        # I don't even have to be finished,  for this
        qb = QueryBuilder()
        qb.append(BranchingCalculation, filters={'id':self.id}, tag='b')
        qb.append(MoldynCalculation, output_of='b', edge_project='label', edge_filters={'type':LinkType.CALL.value, 'label':{'like':'slave_NVE_%'}}, tag='c', edge_tag='mb', project='*')
        d = {item['mb']['label']:item['c']['*'].get_output_trajectory() for item in qb.iterdict()}
        return zip(*sorted(d.items()))[1]


        
        

