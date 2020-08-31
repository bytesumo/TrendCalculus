import sys
import csv
import ray
import numpy as np


# create some ray actor classes, and update functions. Later decorate figure out how to scale up
@ray.remote
class StateArrayBuffer(object):

    def __init__(self):
        # we start by defining the buffer schema
        # later, we will expand the schema of rolling metrics we track,to enable new kinds of IncrementalAggregate
        self.buffer_schema = np.dtype({'names':('offset', 'timeframe', 'rollingsum', 'SMA'),
                                       'formats':(np.uint64, np.uint16, np.float64, np.float64)})
        # here we create the buffer object
        self.state_array = np.zeros(1, dtype=self.buffer_schema)	
        # note the offset counter is initialised at zero

    def get_state(self):
        return self.state_array

    def update_state(self, updated_state):
        self.state_array = updated_state

    def at_tf(self, time_frame):
        return self.state_array[state_array['timeframe'] == time_frame]

@ray.remote
def IncrementalAggregate(mybuffer, EventRecord, raxTimeFrame):
    # This function should be called per event, on an ordered stream of events
    # Note the EventRecord should have a schema, like this example
    #    OrderedDict([('Instrument', 'gbpcad'), ('DateTime', '20190602 170000'), ('Close', '1.707180')])
    # For this event fetch the state for this Instrument. If it doesn't exist, it gets initialised. 

    state = ray.get(StateArrayBuffer(mybuffer).get_state).remote()

    # the state is a numpy array, the max rows should be limited to MaxTimeFrame
    # copy the existing state to a new array which we'll update inplace using values in the event record

    tf_zero = state[0].copy    

    # update the state, increment the offset, timeframe for each row, add Close to RollingSum
    state['offset'] += 1
    state['timeframe'] += 1
    state['rollingsum'] += EventRecord['Close']
    state['SMA'] = state['rollingsum']/state['tf']

    new_state = np.concatenate(tf_zero, state[ state['timeframe'] <= MaxTimeFrame ])

    ray.put(StateArrayBuffer(mybuffer).update_state(new_state)).remote()
    return
    
# with this functions, manage the buffer to calculate the SMAs



# start ray

ray.init()
assert ray.is_initialized() == True


# instantiate my buffer actor

try:
    mybuff = StateArrayBuffer.remote()
except Exception:
    print("Error initialising mybuff = StateArrayBuffer.remote ")

mytf = 20



# kick off a stream to test on 

# this example expects the following schema as csv lines
#OrderedDict([('Instrument', 'gbpcad'), ('DateTime', '20190602 170300'), ('Close', '1.707130')])

inputfile = "../examples/data/gbpcad.csv"

try:
    from csv import DictReader
    # open file in read mode
    with open(inputfile, 'r') as read_obj:
        # pass the file object to DictReader() to get the DictReader object
        csv_dict_reader = DictReader(read_obj)
        # iterate over each line as a ordered dictionary
        for row in csv_dict_reader:
            # row variable is a dictionary that represents a row in csv
            print(row)

except Exception:
    print("error iterating the csv parsed data")




ray.shutdown()
assert ray.is_initialized() == False

