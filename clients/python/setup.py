from setuptools import setup, find_packages

setup(
    name = 'lipstick',
    version = '0.0.1',
    url = 'http://github.com/Netflix/Lipstick/clients/python',
    description = 'Lipstick client library',
    license = 'Apache Software License',
    author = 'Netflix Data Platform Architecture',
    author_email = 'dataplatformarchitecture@netflix.com',
    packages = find_packages(),
    install_requires = [
        'requests>=1.0.0'
    ],
    zip_safe = False
)
