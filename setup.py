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
        url="xxxx",
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

def clean():
    import os
    # os.removedirs('./build')
    base_script = os.path.dirname(os.path.abspath(__file__))
    build_dir = os.path.join(base_script, 'build')
    dist_dir = os.path.join(base_script, 'dist')
    shutil.rmtree(build_dir, ignore_errors=True)
    shutil.rmtree(dist_dir, ignore_errors=True)
    shutil.rmtree(os.path.join(base_script, 'api', 'launcherapi.egg-info'), ignore_errors=True)
    shutil.rmtree(os.path.join(base_script, 'client', 'jdray.egg-info'), ignore_errors=True)
    shutil.rmtree(os.path.join(base_script, 'core', 'launcher.egg-info'), ignore_errors=True)


if __name__ == '__main__':
    # clean()
    package('airflow-ray-executor', 'Airflow executor implemented using ray')
