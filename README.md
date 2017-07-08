# Table of Contents
1. [Challenge Summary](README.md#challenge-summary)
2. [Details of Implementation](README.md#details-of-implementation)
3. [Repo Directory Structure](README.md#Repo-Directory-Structure)


# Challenge Summary

This challenge is to find the anomalous purchases in the streaming log data. An anomalous purchase is defined as the purchase with the purchase amount larger than 3 times of standard deviation of the historical T purchases made by a user's D degrees of social network. 

I used python with Pandas package to code the challenge. The source code process_log.py is under the folder src. 


# Details of Implementation

###Establishing Purchase History

Pandas Dataframe was used to process the batch_log. The batch log was used to establish the purchase history of all users, as well as the social network graph of all users. The former data are store in purch_hist_df, and the latter is stored in a graph described by a Python dictionary, soc_nw. 

###Establishing Social Network

Note that the soc_nw is the 1st degree of the user's social network (ie, direct friends). To define the D degrees of the user's social network. A function is defined as "soc(D, uid)". The two arguments passed to the function are D (int), the degree of social network interested, and uid (str), the id of a user. 

After reading the values of D and T from batch_log.jason and passing D to soc(D, uid), the total D degree connections of a user is obtained in a dictionary "D_soc_nw", 

### Analyzing the Stream Data
dsf
In a for loop, The file stream_log.json is read line-by-line into a dict, stream_line. The user id is read from 'stream_line['id']', D_soc_nw is used to find the total network of this user. The purch_hist_df was filtered by the total network (list of user ids) of the user. Then the filtered data frame was sorted according to the index of the entry, assuming that the entry is chronological. Finding the T's latest purchase entries, the mean and the standard deviation of the amounts of purchasing are calculated.

The amount of purchase in the current entry of stream_line (float(stream_line['amount'])) is then compared with mean + 3 * std, if the float(stream_line['amount']) is larger, then this entry is an anomalous purchase. This entry is then recorded into flagged_purchases.json as an ordered dictionary. 

The new entry of purchase record in the stream date is then appended to the dataframe of purch_hist_df.

### Updating social network

If the event_type is either "befriend" or "unfriend", the social network of a user is then updated and D's degree of total connections are also updated. This information will be passed into the analysis of the next entry in the stream log. 




#Repo Directory Structure

The repo structure is not changed. I did not build my own test for this project:

    ├── README.md 
    ├── run.sh
    ├── src
    │   └── process_log.py
    ├── log_input
    │   └── batch_log.json
    │   └── stream_log.json
    ├── log_output
    |   └── flagged_purchases.json
    ├── insight_testsuite
        └── run_tests.sh
        └── tests
            └── test_1
            |   ├── log_input
            |   │   └── batch_log.json
            |   │   └── stream_log.json
            |   |__ log_output
            |   │   └── flagged_purchases.json
