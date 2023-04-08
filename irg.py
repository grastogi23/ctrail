#!/usr/bin/python3
"""
This is program to iterate over directory of aws cloud trail logs.
"""

from typing import Callable
import argparse
import os
import traceback
import networkx
import ijson
import matplotlib.pyplot as plt

class CtrailStats(dict):
    """Class to hold different stats and improve initialization"""
    def __init__(self) -> None:
        # per code set of error messages
        self['errorCode'] = {}
        self['errorMessage'] = {}
        self['userIdentity'] = {}
        self['userIdentity']['type'] = set()
        self['eventName'] = set()
        self['nodes'] = {}

    def summary(self)->str:
        """
        returns: string of stats
        """
        r = ""
        for k, v in self.items():
            r = r + k + ', ' + str(len(v)) + '\n'
        return r

def idgraph_accumulator(rec: dict, idgraph: networkx.Graph, stats: CtrailStats):
    """
    builds the idgraph
    """
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
    v1_index = None
    try:
        v1_id = rec['userIdentity'].get('arn', rec['userIdentity']['principalId'])
        if v1_id not in stats['nodes']:
            nodeData = {'nodeType': 'userIdentity'}
            v1_index = idgraph.number_of_nodes()
            stats['nodes'][rec['userIdentity']['arn']] = v1_index
            nodeData.update(rec['userIdentity'])
            idgraph.add_node((v1_index, nodeData['arn']))
            print("adding v1: node", rec['eventName'], nodeData, 'at', v1_index)
        else:
            v1_index = stats['nodes'][v1_id]
    except KeyError:
        print('error', rec)

    for resource in rec.get('resources', []):
        v2_index = None
        if resource['ARN'] not in stats['nodes']:
            nodeData = {'nodeType': 'resource'}
            v2_index = idgraph.number_of_nodes()
            nodeData.update(resource)
            nodeData['arn'] = nodeData['ARN']
            del nodeData['ARN']
            stats['nodes'][resource['ARN']] = v2_index
            idgraph.add_node((v2_index, nodeData['arn']))
            print("adding v2: node", rec['eventName'], nodeData, 'at', v2_index)
        else:
            v2_index = stats['nodes'][resource['ARN']]

        # add edge between v1_index and v2_index
        # check if edge exists
        edge_data = idgraph.get_edge_data(v1_index, v2_index)
        #print('adding edge', v1_index, v2_index)
        if not edge_data:
            edge_data = {'events': [rec]}
            idgraph.add_edge(v1_index, v2_index, data=edge_data)
        else:
            edge_data['data']['events'].append([rec])

def cloud_trail_walker(data_dir: str, walk_fn: Callable[..., None], **kwargs)->None:
    """
    # Iterate over the files and read them using stream reader
    # for each file
    #    withoopen
    #       read logs
    #       add to the graph
    """
    for root, _, files in os.walk(data_dir):
        for file in filter(lambda x: x.lower().endswith(".json"), files):
            try:
                with open(os.path.join(root, file), 'r', encoding="utf8") as fhandle:
                    for rec in ijson.items(fhandle, 'Records.item'):
                        walk_fn(rec, **kwargs)
            except OSError as ex:
                traceback.format_exception(ex)

def cloud_trail_idgraph(data_dir: str, skip_graph: bool)->None:
    """
    Fills in the stats and builds idgraph
    """
    stats = CtrailStats()
    idgraph = networkx.Graph()

    cloud_trail_walker(data_dir, idgraph_accumulator, idgraph=idgraph, stats=stats)

    print('num nodes', idgraph.number_of_nodes())
    print('num edges', idgraph.number_of_edges())
    print('Summary\n', stats.summary())
    networkx.draw(idgraph, with_labels = True)
    if not skip_graph:
        plt.show()

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
    parser.add_argument('-s', '--skip_graph', default=False, action="store_true", help='show graph')
    args = parser.parse_args()
    s_dir = args.data_dir
    is_skip_graph = args.skip_graph
    print('building graph', s_dir, "show graph", is_skip_graph)
    cloud_trail_idgraph(s_dir, is_skip_graph)
