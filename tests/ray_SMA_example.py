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
        state_array = np.zeros(1, dtype=self.buffer_schema)
        self.state_array_ref = ray.put(state_array)	
        # note the offset counter is initialised at zero

    def get_state(self):
        state_array = ray.get(self.state_array_ref)
        return state_array

    def update_state(self, EventRecord, MaxTimeFrame):
        old_state_array = ray.get(self.state_array_ref)
        tmp_state_array = old_state_array.copy()
        offset = old_state_array['offset'][0:2].max(axis = 0) + 1
        tmp_state_array['offset'] = offset
        tmp_state_array['timeframe'] += 1
        tmp_state_array['rollingsum'] += float(EventRecord['Close'])
        tmp_state_array['SMA'] = tmp_state_array['rollingsum']/tmp_state_array['timeframe']
        # initialise a new zero timeframe 
        tf_zero = np.zeros(1, dtype=self.buffer_schema)
        # append the recalc of the state, truncated to max time frame
        new_state = np.concatenate((tf_zero, tmp_state_array[tmp_state_array['timeframe'] <= MaxTimeFrame ]))
        # save the new array, and store the reference here in the actor
        self.state_array_ref = ray.put(new_state)
        return 

    def get_tf(self, time_frame):
        curr_state_array = ray.get(self.state_array_ref)
        tmp_state_array = curr_state_array.copy()
        just_requested_tf = tmp_state_array[tmp_state_array['timeframe'] == int(time_frame)]
        return just_requested_tf

    def get_all_tf(self):
        curr_state_array = ray.get(self.state_array_ref)
        all_tfs = curr_state_array['SMA'][2:-1]
        offset = curr_state_array['offset'][1]
        return offset, all_tfs

    def get_offset(self, time_frame):
        curr_state_array = ray.get(self.state_array_ref)
        curr_offset = curr_state_array['offset'][1]
        return curr_offset
# start ray

ray.init()
assert ray.is_initialized() == True

# kick off a stream to test on 
# this example expects the following schema as csv lines
#OrderedDict([('Instrument', 'gbpcad'), ('DateTime', '20190602 170300'), ('Close', '1.707130')])
#        curr_max_tf = tmp_state_array['offset'].max(axis=0)

inputfile = "../examples/data/gbpcad.csv"

# instantiate my buffer actor
all_windows = StateArrayBuffer.remote()
max_tf = 20

from csv import DictReader
# open file in read mode

with open(inputfile, 'r') as read_obj:
    # pass the file object to DictReader() to get the DictReader object

    csv_dict_reader = DictReader(read_obj)

    # iterate over each line as a ordered dictionary
    for row in csv_dict_reader:

        # submit our new record to update the ray state_buffer actor
        ray.get(all_windows.update_state.remote(row, max_tf))
        matrix = ray.get(all_windows.get_state.remote())
        sma_tf = ray.get(all_windows.get_tf.remote(max_tf))
        offset, sma_all = ray.get(all_windows.get_all_tf.remote())
        sma_all_csv = ','.join(['%.5f' % num for num in sma_all])
        
        print(row['Instrument'], row['DateTime'], row['Close'], str(offset), sma_all_csv, sep=",")
        #print('offset', 'timeframe', 'rollingsum', 'SMA', sep="\t")
        #print('\n'.join(['\t'.join([str(cell) for cell in row]) for row in matrix]))

ray.shutdown()
assert ray.is_initialized() == False
