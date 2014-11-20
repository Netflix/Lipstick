def template(data):
    return Template(data.pop('name'), **data)

class Template():
    def __init__(self, name, view=None, template=None):
        self.name = name
        self.view = view
        self.template = template

    def view(self, view):
        self.view = view
        return self

    def template(self, template):
        self.template = template
        return self

    def load_view(self, view_file):
        self.view = open(view_file).read()
        return self

    def load_template(self, template_file):
        self.template = open(template_file).read()
        return self

    def json(self):
        return json.dumps(self.__dict__)
