from aiida.orm.calculation.chillstep.fibonacci import FibonacciCalculation, FibonacciRecCalculation
from aiida.orm.calculation.chillstep import tick_chillstepper, run
from aiida.orm.data.base import Int
from aiida.orm.data.parameter import ParameterData
params = Int(10)


#~ fc = FibonacciRecCalculation(parameters=params)
fc = FibonacciCalculation(parameters=params)

fc.submit()
print "created", fc



#~ store = True
#~ if store:

#~ run(fc, store=True)

#~ print fc._plugin_type_string
#~ print fc, fc.get_attrs(), fc.get_outputs()
#~ for i in range(4):
    #~ tick_chillstepper(fc)
