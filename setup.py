import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname), "r").read()

def get_version():
    g = {}
    exec(open(os.path.join("director", "version.py"), "r").read(), g)
    return g["Version"]


setup(
    name = "director",
    version = get_version(),
    author = "Igor Mandrichenko",
    author_email = "ivm@fnal.gov",
    description = ("A tool to run shell commands in parallel/sequential groups."),
    license = "BSD 3-clause",
    keywords = "parallel execution, shell",
    url = "https://github.com/ivmfnal/director",
    packages=['director'],
    include_package_data = True,
    install_requires=["pythreader", "lark"],
    zip_safe = False,
    classifiers=[
    ],
    entry_points = {
            "console_scripts": [
                "director = director.director:main",
            ]
        }
)