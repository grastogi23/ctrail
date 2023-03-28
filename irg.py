#!/usr/bin/python3


import argparse
import os
import networkx
import ijson


def idgraph_build(rec: dict, idgraph: networkx.Graph, stats: dict):
    stats['userIdentity']['type'].add(rec['userIdentity'].get('type','unknown'))
    stats['eventName'].add(rec['eventName'])
    # dictionary of users with index and user.    
    try:
        if rec['errorCode'] not in stats['errorCode']:
                stats['errorCode'][rec['errorCode']] = set()
        stats['errorCode'][rec['errorCode']].add(rec['errorMessage'].split('.')[0])
        # ignore the error scenarios
        return
    except KeyError:
        pass
    if rec['userIdentity'].get('type') != "IAMUser":
        return
    try:
        if rec['userIdentity'].get('arn', rec['userIdentity']['principalId']) not in stats['nodes']:
            nodeData = {'nodeType': 'userIdentity'}
            node_index = idgraph.number_of_nodes()
            stats['nodes'][rec['userIdentity']['arn']] = node_index
            nodeData.update(rec['userIdentity'])
            idgraph.add_node((node_index, nodeData['arn']))
            print("adding", rec['eventName'], nodeData, 'at', node_index)
    except KeyError as e:
        print('error', rec)
        pass
    for resource in rec.get('resources', []):
        if resource['ARN'] not in stats['nodes']:
            nodeData = {'nodeType': 'resource'}
            node_index = idgraph.number_of_nodes()
            nodeData.update(resource)
            nodeData['arn'] = nodeData['ARN']
            del nodeData['ARN']
            stats['nodes'][resource['ARN']] = node_index
            idgraph.add_node((node_index, nodeData['arn']))
            print("adding", rec['eventName'], nodeData, 'at', node_index)
    return


def cloud_trail_walker(data_dir: str):
    # Iterate over the files and read them using stream reader
    # for each file
    #    withoopen
    #       read logs
    #       add to the graph
    # 

    stats = dict()
    # per code set of error messages
    stats['errorCode'] = {}
    stats['errorMessage'] = {}
    stats['userIdentity'] = {}
    stats['userIdentity']['type'] = set()
    stats['eventName'] = set()
    stats['nodes'] = {}

    idgraph = networkx.Graph()
    # Node -> Identity
    # Resoure -> 
    # Edge is the transaction

    for root, dirs, files in os.walk(data_dir):
        for file in filter(lambda x: x.lower().endswith(".json"), files):
            with open(os.path.join(root, file), 'r') as f:
                for rec in ijson.items(f, 'Records.item'):
                    idgraph_build(rec, idgraph, stats)
        #print(root, dirs, files)

    print(stats['userIdentity'], len(stats['errorCode']))

    return idgraph


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='Identity Relationship Graph',
        description='shows graph from the cloud trail',
        usage="""
        python irg.py --data ./data
        output: networkx
        """
    )

    parser.add_argument('-d', '--data_dir', help='location of the data files')

    args = parser.parse_args()

    data_dir = args.data_dir

    print('building graph', data_dir)
    idgraph = cloud_trail_walker(data_dir)
    print('num nodes', idgraph.number_of_nodes())