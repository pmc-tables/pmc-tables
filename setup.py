#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

def _read_md_as_rst(file):
    """Read Markdown file and convert it to ReStructuredText."""
    from pypandoc import convert_file
    return convert_file(file, 'rst', format='md')


def _read_md_as_md(file):
    """Read Markdown file."""
    with open(op.join(op.dirname(__file__), file)) as ifh:
        return ifh.read()


def read_md(file):
    """Read MarkDown file and try to convert it to ReStructuredText if you can."""
    try:
        return _read_md_as_rst(file)
    except ImportError:
        warnings.warn("pypandoc module not found, could not convert Markdown to RST!")
        return _read_md_as_md(file)


requirements = [
    'Click>=6.0',
    # TODO: put package requirements here
]

setup_requirements = [
    'pytest-runner',
    # TODO(ostrokach): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    'pytest',
    # TODO: put package test requirements here
]

setup(
    name='pmc_tables',
    version='0.1.0',
    description="Extract relational tables from PubMed Central.",
    long_description=read_md('README.md') + '\n\n' + read_md('HISTORY.md'),
    author="Alexey Strokach",
    author_email='alex.strokach@utoronto.ca',
    url='https://gitlab.com/ostrokach/pmc_tables',
    packages=find_packages(include=['pmc_tables']),
    entry_points={
        'console_scripts': [
            'pmc_tables=pmc_tables.cli:main'
        ]
    },
    include_package_data=True,
    install_requires=requirements,
    zip_safe=False,
    keywords='pmc_tables',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
