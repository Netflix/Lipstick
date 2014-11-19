import requests

class Client():    
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
        return self.request('post', path, data=data, **kwargs)
