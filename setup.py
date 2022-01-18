#!/usr/bin/env python
# encoding: utf-8
import shutil

import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

def package(name, desc):
    setuptools.setup(
        name=name,
        version="0.0.1",
        author="staneyffer",
        author_email="",
        description=desc,
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="https://github.com/staneyffer/airflow-ray-executor",
        packages=setuptools.find_packages(),
        install_requires=[
            "ray"
        ],
        entry_points={
            'console_scripts': [
                'douyin_image=douyin_image:main'
            ],
        },
        classifiers=[
            "Topic :: Software Development :: Libraries :: Python Modules",
            "Operating System :: OS Independent",
            "Programming Language :: Python",
            "Programming Language :: Python :: 3.6",
            "Programming Language :: Python :: 3.7",
            "Programming Language :: Python :: 3.8",
        ],
    )

if __name__ == '__main__':
    package('airflow-ray-executor', 'Airflow executor implemented using ray')
