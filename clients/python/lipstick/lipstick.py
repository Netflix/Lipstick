import copy
import json
from time import time
from .client import Client

def node(data):
    return Node(data.pop('id'), **data)

def edge(data):
    return Edge(data.pop('u'), data.pop('v'), **data)

def node_group(data):
    return NodeGroup(data.pop('id'), **data)

def status(data):
    return Status(**data)
    
def graph(data):
    return Graph(data.pop('id'), **data)
    
class Graph():
    def __init__(self, id, name=None, **kwargs):
        self.id = id
        self.name = name
        self.status = kwargs.get('status', Status())
        self.properties = kwargs.get('properties', {})
        self.set_nodes(kwargs.get('nodes', []))
        self.set_edges(kwargs.get('edges', []))
        self.set_node_groups(kwargs.get('node_groups', []))

    def add_node(self, node):
        self.nodes[node.id] = node
        return self

    def add_edge(self, edge):
        self.edges[edge.u+edge.v] = edge
        return self

    def add_node_group(self, node_group):
        self.node_groups[node_group.id] = node_group
        return self

    def get_node(self, node_id):
        return self.nodes[node_id]

    def get_edge(self, u, v):
        return self.edges[u+v]

    def get_node_group(self, ng_id):
        return self.node_groups[ng_id]
        
    def set_nodes(self, nodes):
        self.nodes = dict([(n['id'], node(n)) for n in nodes])

    def set_edges(self, edges):
        self.edges = dict([(e['u']+e['v'], edge(e)) for e in edges])

    def set_node_groups(self, node_groups):
        self.node_groups = dict([(ng['id'], node_group(ng)) for ng in node_groups])
        
    def json(self):
        data = copy.copy(self.__dict__)
        data['status'] = self.status._to_dict()
        data['nodes'] = [n._to_dict() for n in self.nodes.values()]
        data['edges'] = [e._to_dict() for e in self.edges.values()]
        data['node_groups'] = [ng._to_dict() for ng in self.node_groups.values()]        
        return json.dumps(data)

    def save(self, url):
        client = Client(url)
        return client.post('/v1/job', data=self.json())
    
class Node():
    def __init__(self, id, url=None, type=None, child=None, **kwargs):
        self.id = id
        self.url = url
        self.type = type
        self.status = kwargs.get('status', Status())
        self.properties = kwargs.get('properties', {})
        
    def _to_dict(self):
        result = copy.copy(self.__dict__)
        result['status'] = self.status._to_dict()
        return result
        
class Edge():
    def __init__(self, u, v, label=None, type=None, **kwargs):
        self.u = u
        self.v = v
        self.type = type
        self.label = label
        self.properties = kwargs.get('properties', {})

    def _to_dict(self):
        return self.__dict__

class NodeGroup():
    def __init__(self, id, url=None, **kwargs):
        self.id = id
        self.url = url
        self.status = kwargs.get('status', Status())
        self.children = kwargs.get('children', [])
        self.properties = kwargs.get('properties', {})

    def _to_dict(self):
        result = copy.copy(self.__dict__)
        result['status'] = self.status._to_dict()
        return result

class Status():
    def __init__(self, progress=0, endTime=None, statusText=None, **kwargs):
        now = int(round(time()*1000.0))
        self.startTime = kwargs.get('startTime', now)
        self.heartbeatTime = kwargs.get('heartbeatTime', now)    

    def _to_dict(self):
        return self.__dict__
