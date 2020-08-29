import numpy as np
import ray
import time

#####################################################################################################
# trendcalculus
#
# Example of sliding window aggregates, made incremental using all timeframes
#
# This example will compute a simple moving average on every timeframe from 1 to MaxTimeFrame
# The data will be read in using STDIN with the readstdin.py code, and these functions will be called.
#
######################################################################################################


ray.init()    # Start Ray

# this is an actor, that manages the state of the buffer, tracking aggregates on all timeframes 
# up to MaxTimeFrame
@ray.remote
class StateArrayBuffer(TimeSeriesName):

    def __init__(self):
        # later, we will expand the schema of rolling metrics we track,to enable new kinds of IncrementalAggregate
        self.buffer_schema = np.dtype({'names':('offset', 'timeframe', 'rollingsum', 'SMA'),
                                       'formats':(np.uint64, np.uint16, np.float64, np.float64)})
        self.state_array = np.zeros(1, dtype=buffer_schema)
        # note the offset counter is also initialised at zero
        # self.state_array[0,0]=0

    def get_state(self):
        return self.state_array

    def update_state(self, updated_state):
        self.state_array = updated_state


# this is the Incremental Aggregate fn (a reduce function), that adds one new value to running aggregates
# it runs on the change of an input value, gets and updates the state, and stashes it
@ray.remote  
def IncrementalAggregate(EventRecord, MaxTimeFrame):
    # This function should be called per event, on an ordered stream of events
    # Note the EventRecord should have a schema, like this example
    #    OrderedDict([('Instrument', 'gbpcad'), ('DateTime', '20190602 170000'), ('Close', '1.707180')])

    # For this event fetch the state for this Instrument. If it doesn't exist, it gets initialised. 
    state = ray.get(StateArrayBuffer(EventRecord('Instrument')).get_state.remote())

    # the state is a numpy array, the max rows should be limited to MaxTimeFrame
    # copy the existing state to a new array which we'll update inplace using values in the event record

    tf_zero = state[0].copy    

    # update the state, increment the offset, timeframe for each row, add Close to RollingSum
    state['offset'] += 1
    state['timeframe'] += 1
    state['rollingsum'] += EventRecord['Close']
    state['SMA'] = state['rollingsum']/state['tf']
    
    new_state = np.concatenate(tf_zero, state[state['timeframe'] <= MaxTimeFrame])

    ray.put(StateArrayBuffer(EventRecord('Instrument')).update_state(new_state).remote()


# Start two parameter servers, each with half of the parameters.
#parameter_servers = [ParameterServer.remote(5) for _ in range(2)]

# Start 2 workers.
#workers = [
#    sharded_worker.remote(*parameter_servers) for _ in range(2)]

# Inspect the parameters at regular intervals until we've 
# reached the end (i.e., each parameter equals 200)
#while True:
#    time.sleep(1)
#    results = ray.get(
#        [ps.get_params.remote() for ps in parameter_servers])
#    print(results)
#    if results[0][0] >= 200.0:
#        break
