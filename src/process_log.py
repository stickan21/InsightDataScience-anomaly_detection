import json
import os
import pandas as pd
import sys
import numpy as np
from collections import OrderedDict


###### Load data into Pandas dataframes. ###############
data_type = {"D": int, "T": int, "event_type": str, "timestamp": 'datetime64[ns]', "id": str, "amount": float, \
             'id1': str, 'id2': str}
stream_df = pd.read_json(sys.argv[2], dtype = data_type, lines = True)
batch_df_raw = pd.read_json(sys.argv[1], dtype = data_type, lines = True)


D, T = batch_df_raw.iloc[0, 0:2]

D = int(D)
T = int(T)


batch_df = batch_df_raw.drop(batch_df_raw.index[0]).drop(['D', 'T'], 1)


####### Establish Social Network ########

befriend_df = batch_df[batch_df.event_type == 'befriend'].drop(['amount', 'id'], 1)
unfriend_df = batch_df[batch_df.event_type == 'unfriend'].drop(['amount', 'id'], 1)



###### Finding the 1st degree connections of each user (D = 1) ###########

soc_nw = {}
for userid in np.unique(befriend_df[['id1', 'id2']].values):
    soc_nw[userid] = []



for i in befriend_df.index:
    soc_nw[befriend_df.at[i, 'id1']].append(befriend_df.at[i, 'id2'])
    soc_nw[befriend_df.at[i, 'id2']].append(befriend_df.at[i, 'id1'])

for i in unfriend_df.index:
    soc_nw[unfriend_df.at[i, 'id1']].remove(unfriend_df.at[i, 'id2'])
    soc_nw[unfriend_df.at[i, 'id2']].remove(unfriend_df.at[i, 'id1'])


############ Finding the Dth degree connections of User 'uid' ###############

def soc(D, uid):
    notes = np.array([])
    if D == 1:
        return np.array(soc_nw[uid])
    elif D > 1:
        for i in soc(D-1, uid).flatten():
            if i == uid:
                continue
            notes = np.append(notes, soc_nw[i]) 
        return np.setdiff1d(np.unique(notes.flatten()), [uid])
    else:
        return False


########### Find the D degrees' total connections of each user ##########

D_soc_nw = {}
for user in soc_nw.keys():
    d_conn = np.array([])
    for d in range(1, D + 1):
        d_conn = np.unique(np.concatenate((d_conn, soc(d, user)),0))
        D_soc_nw[user] = d_conn


######## Establish Purchase Histroy ########

######## If not using DataFrame ##########
#purch_hist = {}
#soc_ntwk = {}
#with open('batch_log.json', 'r') as batch:
#    for line in batch:
#       batch_line = (json.loads(line))
#       if batch_line.has_key('event_type') and batch_line['event_type'] == 'purchase':
#            if not purch_hist.has_key(batch_line['id']):
#                purch_hist[batch_line['id']] = {}
#                purch_hist[batch_line['id']]['timestamp'] = []
#                purch_hist[batch_line['id']]['amount'] = []
#            purch_hist[batch_line['id']]['timestamp'].append(batch_line['timestamp'])
#            purch_hist[batch_line['id']]['amount'].append(batch_line['amount'])


purch_hist_df = batch_df[batch_df.event_type == 'purchase'].drop(['id1', 'id2'], 1)


######## Analysis of Stream Data ############

data_type_stream_purch = {"event_type": str, "timestamp": 'datetime64[ns]', "id": str, "amount": float}


with open(sys.argv[3], 'a') as fp:
    with open(sys.argv[2], 'r') as stream:

        for line in stream:

            if line == '\n':
                break

            stream_line =json.loads(line, object_pairs_hook=OrderedDict)

            #Find the T times of purchases in D degrees of soc_nw of user.
            if stream_line['event_type'] == 'purchase':
                purch_hist_conn = purch_hist_df[purch_hist_df.id.isin(D_soc_nw[stream_line['id']])]
                #purch_hist_conn['index'] = purch_hist_conn.index
                purch_hist_conn = purch_hist_conn.sort_index(ascending = False).iloc[0:T, :]

                #Calculate the mean and std.
                purch_mean = purch_hist_conn.amount.mean()
                purch_sd = purch_hist_conn.amount.std(ddof = 0)

                #Append the new entry to purch_hist.
                purch_hist_df = purch_hist_df.append(pd.DataFrame([stream_line.values()], columns = stream_line.keys()) \
                                                     .astype(data_type_stream_purch), ignore_index = True)

                #Evaluate anomaly and write to json.
                if float(stream_line['amount']) > purch_mean + 3 * purch_sd:
                    flagged_keys = stream_line.keys()
                    flagged_values = stream_line.values()
                    flagged_keys.extend(['mean', 'sd'])
                    flagged_values.extend([str(format(purch_mean, '.2f')), str(format(purch_sd, '.2f'))])
                    flagged_entry = OrderedDict(zip(flagged_keys, flagged_values))

                    json.dump(flagged_entry, fp)
                    fp.write('\n')

            #Update the soc_nw of user when seeing a "befriend" or "unfriend" entry.
            elif stream_line['event_type'] == 'befriend':
                soc_nw[stream_line['id1']].append(stream_line['id2'])
                soc_nw[stream_line['id2']].append(stream_line['id1'])
                d_conn_id1 = np.array([])
                d_conn_id2 = np.array([])
                for d in range(1, D + 1):
                    d_conn_id1 = np.unique(np.concatenate((d_conn_id1, soc(d, stream_line['id1'])),0))
                    D_soc_nw[stream_line['id1']] = d_conn_id1
                    d_conn_id2 = np.unique(np.concatenate((d_conn_id2, soc(d, stream_line['id2'])),0))
                    D_soc_nw[stream_line['id2']] = d_conn_id2

            elif stream_line['event_type'] == 'unfriend':
                soc_nw[stream_line['id1']].remove(stream_line['id2'])
                soc_nw[stream_line['id2']].remove(stream_line['id1'])
                d_conn_id1 = np.array([])
                d_conn_id2 = np.array([])
                for d in range(1, D + 1):
                    d_conn_id1 = np.unique(np.concatenate((d_conn_id1, soc(d, stream_line['id1'])),0))
                    D_soc_nw[stream_line['id1']] = d_conn_id1
                    d_conn_id2 = np.unique(np.concatenate((d_conn_id2, soc(d, stream_line['id2'])),0))
                    D_soc_nw[stream_line['id2']] = d_conn_id2