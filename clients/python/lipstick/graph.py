import json
import copy
from time import time

def node(data):
    return Node(data.pop('id'), **data)

def edge(data):
    return Edge(data.pop('u'), data.pop('v'), **data)

def node_group(data):
    return NodeGroup(data.pop('id'), **data)

def status(data):
    return Status(**data)

def stage(data):
    return Stage(data.pop('name'), **data)
    
def graph(data):
    return Graph(data.pop('id'), **data)
    
class Graph():
    def __init__(self, id, name=None, user=None, **kwargs):
        self._id = id
        self._name = name
        self._user = user
        self._status = status(kwargs.get('status', {}))
        self._properties = kwargs.get('properties', {})
        self._nodes = dict()
        self._edges = dict()
        self._node_groups = dict()
        self.nodes(kwargs.get('nodes', []))
        self.edges(kwargs.get('edges', []))
        self.node_groups(kwargs.get('node_groups', []))        

    def id(self, id=None):
        if (id):
            self._id = id
            return self
        else:
            return self._id
        
    def name(self, name=None):
        if (name):
            self._name = name
            return self
        else:
            return self._name

    def user(self, user=None):
        if (user):
            self._user = user
            return self
        else:
            return self._user

    def status(self, status=None):
        if (status):
            self._status = status
            return self
        else:
            return self._status

    def node(self, node):        
        self._nodes[node.id()] = node
        return self

    def edge(self, edge):
        self._edges[edge.u()+edge.v()] = edge
        return self

    def node_group(self, node_group):
        self._node_groups[node_group.id()] = node_group
        return self
        
    def get_node(self, node_id):
        return self._nodes[node_id]

    def get_edge(self, u, v):
        return self._edges[u+v]

    def get_node_group(self, ng_id):
        return self._node_groups[ng_id]
        
    def nodes(self, nodes=None):
        if (nodes):
            self._nodes = dict([(n['id'], node(n)) for n in nodes])
            return self
        else:
            return self._nodes.values()

    def edges(self, edges=None):
        if (edges):
            self._edges = dict([(e['u']+e['v'], edge(e)) for e in edges])
            return self
        else:
            return self._edges.values()

    def node_groups(self, node_groups=None):
        if (node_groups):
            self._node_groups = dict([(ng['id'], node_group(ng)) for ng in node_groups])
            return self
        else:
            return self._node_groups.values()
        
    def json(self):
        data = dict()
        data['id'] = self._id
        if (self._name): data['name'] = self._name
        if (self._user): data['user'] = self._user
        data['properties'] = self._properties
        data['status'] = self._status._to_dict()
        data['nodes'] = [n._to_dict() for n in self.nodes()]
        data['edges'] = [e._to_dict() for e in self.edges()]
        data['node_groups'] = [ng._to_dict() for ng in self.node_groups()]        
        return json.dumps(data)
        
class Node():
    def __init__(self, id, url=None, type=None, child=None, **kwargs):
        self._id = id
        self._url = url
        self._type = type
        self._child = child
        self._status = status(kwargs.get('status', {}))
        self._properties = kwargs.get('properties', {})

    def id(self, id=None):
        if (id):
            self._id = id
            return self
        else:
            return self._id
            
    def status(self, status=None):
        if (status):
            self._status = status
            return self
        else:
            return self._status
            
    def url(self, url=None):
        if (url):
            self._url = url
            return self
        else:
            return self._url

    def type(self, type=None):
        if (type):
            self._type = type
            return self
        else:
            return self._type

    def child(self, child=None):
        if (child):
            self._child = child
            return self
        else:
            return self._child

    def property(self, key, value):
        self._properties[key] = value
        return self

    def get_property(self, key):
        if (key in self._properties):
            return self._properties[key]
        else:
            return
        
    def _to_dict(self):
        data = dict()
        data['id'] = self._id
        data['status'] = self._status._to_dict()
        data['properties'] = self._properties
        if (self._child): data['child'] = self._child
        if (self._url): data['url'] = self._url
        if (self._type): data['type'] = self._type
        return data
        
class Edge():
    def __init__(self, u, v, label=None, type=None, **kwargs):
        self._u = u
        self._v = v
        self._type = type
        self._label = label
        self._properties = kwargs.get('properties', {})

    def u(self, u=None):
        if (u):
            self._u = u
            return self
        else:
            return self._u

    def v(self, v=None):
        if (v):
            self._v = v
            return self
        else:
            return self._v

    def type(self, type=None):
        if (type):
            self._type = type
            return self
        else:
            return self._type

    def label(self, label=None):
        if (label):
            self._label = label
            return self
        else:
            return self._label

    def property(self, key, value):
        self._properties[key] = value
        return self

    def get_property(self, key):
        if (key in self._properties):
            return self._properties[key]
        else:
            return
        
    def _to_dict(self):
        data = dict()
        data['u'] = self._u
        data['v'] = self._v
        data['properties'] = self._properties
        if (self._type): data['type'] = self._type
        if (self._label): data['label'] = self._label
        return data

class NodeGroup():
    def __init__(self, id, url=None, **kwargs):
        self._id = id
        self._url = url
        self._stages = []
        self.stages(kwargs.get('stages', []))
        self._status = status(kwargs.get('status', {}))
        self._children = kwargs.get('children', [])
        self._properties = kwargs.get('properties', {})

    def id(self, id=None):
        if (id):
            self._id = id
            return self
        else:
            return self._id
        
    def url(self, url=None):
        if (url):
            self._url = url
            return self
        else:
            return self._url

    def stage(self, stage):        
        self._stages.append(stage)
        return self
        
    def stages(self, stages=None):
        if (stages):
            self._stages = [stage(s) for s in stages]
            return self
        else:
            return self._stages
            
    def status(self, status=None):
        if (status):
            self._status = status
            return self
        else:
            return self._status

    def children(self, children=None):
        if (children):
            self._children = children
            return self
        else:
            return self._children

    def child(self, node_id):
        self._children.append(node_id)
        return self

    def has_child(self, node_id):
        return node_id in self._children
        
    def property(self, key, value):
        self._properties[key] = value
        return self

    def get_property(self, key):
        if (key in self._properties):
            return self._properties[key]
        else:
            return
        
    def _to_dict(self):
        data = dict()
        data['id'] = self._id
        data['status'] = self._status._to_dict()
        data['children'] = self._children
        data['properties'] = self._properties
        data['stages'] = [s._to_dict() for s in self._stages]
        if (self._url): data['url'] = self._url        
        return data

class Stage():
    def __init__(self, name, **kwargs):
        self._name = name
        self._status = status(kwargs.get('status', {}))

    def name(self, name=None):
        if (name):
            self._name = name
            return self
        else:
            return self._name

    def status(self, status=None):
        if (status):
            self._status = status
            return self
        else:
            return self._status

    def _to_dict(self):
        data = dict()
        data['name'] = self._name
        data['status'] = self._status._to_dict()
        return data
        
class Status():
    def __init__(self, progress=0, endTime=None, statusText=None, **kwargs):
        now = int(round(time()*1000.0))
        self._progress = progress
        self._endTime = endTime
        self._statusText = statusText
        self._startTime = kwargs.get('startTime', now)
        self._heartbeatTime = kwargs.get('heartbeatTime', now)    

    def progress(self, progress=None):
        if (progress):
            self._progress = progress
            return self
        else:
            return self._progress

    def startTime(self, startTime=None):
        if (startTime):
            self._startTime = startTime
            return self
        else:
            return self._startTime

    def endTime(self, endTime=None):
        if (endTime):
            self._endTime = endTime
            return self
        else:
            return self._endTime
        
    def statusText(self, statusText=None):
        if (statusText):
            self._statusText = statusText
            return self
        else:
            return self._statusText
        
    def heartbeatTime(self, heartbeatTime=None):
        if (heartbeatTime):
            self._heartbeatTime = heartbeatTime
            return self
        else:
            return self._heartbeatTime
        
    def update(self, status):
        self.__dict__.update(status.__dict__)
        return self
        
    def _to_dict(self):
        data = dict()
        if (self._progress): data['progress'] = self._progress
        if (self._startTime): data['startTime'] = self._startTime
        if (self._endTime): data['endTime'] = self._endTime
        if (self._heartbeatTime): data['heartbeatTime'] = self._heartbeatTime        
        return data
