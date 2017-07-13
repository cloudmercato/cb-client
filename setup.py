from setuptools import setup, find_packages
import cb_client


def read_file(name):
    with open(name) as fd:
        return fd.read()

setup(
    name="cb-client",
    version=cb_client.__version__,
    author=cb_client.__author__,
    author_email=cb_client.__email__,
    description=cb_client.__doc__,
    url=cb_client.__url__,
    keywords=cb_client.__keywords__,
    license=cb_client.__license__,
    py_modules=['cb_client'],
    packages=find_packages(),
    install_requires=read_file('requirements.txt').splitlines(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        'Operating System :: OS Independent',
        "Programming Language :: Python",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Topic :: Software Development',
        'Topic :: Software Development :: Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    long_description=read_file('README.rst'),
    entry_points={'console_scripts': [
        'cb-client = cb_client.console:main',
    ]},
)
