import requests
from .graph import *
from .template import Template

class BaseClient(object):    
    def __init__(self, base_url):
        if base_url.startswith("http"):
            self.base_url = base_url
        else:
            self.base_url = "http://"+base_url 

    def request(self, method, path, **kwargs):
        url = self.base_url+path
        return requests.request(method=method, url=url, **kwargs)
        
    def get(self, path, **kwargs):
        kwargs.setdefault('allow_redirects', True)
        return self.request('get', path, **kwargs)

    def post(self, path, data=None, **kwargs):
        return self.request('post', path, data=data, **kwargs)

    def put(self, path, data=None, **kwargs):
        return self.request('put', path, data=data, **kwargs)
        
class Client(BaseClient):
    job_path = '/v1/job'
    template_path = '/template'
    
    def get(self, graph_id):
        path = '%s/%s' % (Client.job_path, graph_id)
        response = super(Client, self).get(path)
        if (response.ok):
            return graph(response.json())
        else:
            response.raise_for_status()

    def list(self):
        response = super(Client, self).get(Client.job_path)
        if (response.ok):
            return response.json()
        else:
            response.raise_for_status()
        
    def save(self, graph):
        return self.post(Client.job_path, data=graph.json())

    def update(self, graph):
        path = '%s/%s' % (Client.job_path, graph.id())
        return self.put(path, data=graph.json())

    def create_template(self, template):
        path = '%s/%s' % (Client.template_path, template.name)
        return self.post(path, data=template.json())

    def get_template(self, name):
        path = '%s/%s' % (Client.template_path, name)
        response = super(Client, self).get(path)
        if (response.ok):
            return template(response.json())
        else:
            response.raise_for_status()
            
    def list_templates(self):
        response = super(Client, self).get(Client.template_path)
        if (response.ok):
            return response.json()
        else:
            response.raise_for_status()        
