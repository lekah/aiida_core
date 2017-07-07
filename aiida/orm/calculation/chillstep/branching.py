from . import ChillstepCalculation
from moldyn import MoldynCalculation
from aiida.orm.data.parameter import ParameterData
from aiida.orm import Data, load_node



class BranchingCalculation(ChillstepCalculation):
    """
    Run a Molecular Dynamics calculations
    """
    def start(self):
        print "starting"
        # Get the parameters
        self.ctx.nr_of_branches = self.inputs.parameters_branching.dict.nr_of_branches
        self.ctx.steps_NVT = self.inputs.moldyn_parameters_nvt.dict.nstep
        self.ctx.thermalization_steps = self.inputs.parameters_branching.dict.thermalization_steps
        self.goto(self.thermalize)


    def thermalize(self):
        # all the settings are the same for thermalization, NVE and NVT
        inp_d = {k:v for k,v in self.get_inputs_dict().items() if not 'parameters_' in k}
        inp_d['moldyn_parameters'] = self.inp.moldyn_parameters_thermalize
        inp_d['parameters'] = self.inp.parameters_thermalize
        self.goto(self.run_NVT)
        return {'thermalizer':MoldynCalculation(**inp_d)}

    def run_NVT(self):
        inp_d = {k:v for k,v in self.get_inputs_dict().items() if not 'parameters_' in k}
        inp_d['moldyn_parameters'] = self.inp.moldyn_parameters_NVT
        inp_d['parameters'] = self.inp.parameters_NVT
        self.goto(self.run_NVE)
        return {'slave_NVT':MoldynCalculation(**inp_d)}

    def run_NVE(self):
        inp_d = {k:v for k,v in self.get_inputs_dict().items() if not 'parameters_' in k}
        inp_d['moldyn_parameters'] = self.inp.moldyn_parameters_NVT
        inp_d['parameters'] = self.inp.parameters_NVT

        traj = self.out.slave_NVT.get_output_trajectory(store=True)

        trajlen = traj.get_positions().shape[0]
        block_length =  1.0*trajlen / self.ctx.nr_of_branches
        
        indices = [int(i*block_length)-1 for i in range(1, nr_of_branches+1)]

        for count, idx in enumerate(indices):
            res = get_structure_from_trajectory_for_pinball_inline(
                    structure=struc, trajectory=traj,
                    parameters=get_or_create_parameters(dict(
                            step_index=idx,
                            pos_units=traj.get_attr('units|positions'),
                            vel_units=traj.get_attr('units|velocities'),
                            recenter=True
                    )), store=submit)

    def run_NVE(self):
        
        

