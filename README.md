![Graphical Introduction to Eden](docs/_static/header.png)
Eden
==============================
[//]: # (Badges)
[![Documentation Status](https://readthedocs.org/projects/eden/badge/?version=latest)](https://eden.readthedocs.io/en/latest/?badge=latest)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/davidkastner/eden.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/davidkastner/eden/context:python)
[![codecov](https://codecov.io/gh/davidkastner/eden/branch/main/graph/badge.svg?token=Yg75J11uPa)](https://codecov.io/gh/davidkastner/eden)

---
Welcome to Eden! A machine learning initiative to identify the perfect city. 
The project seeks to build a ML model based on hundreds of compiled features for every city in the United States.
The code is organized as a library and can be customized. However, default profiles will also be included.

## Installation:
Eden is built as both a library for building custom models leveraging the Eden database, 
and a click and run software to get instant results. 
However, despite the ambitious nature of the project the setup has been optimized to be as simple as possible.

The first thing you should do is to setup a virutal enviroment (VE). 
While this is not necessary, it is good practice and may help prevent annoying dependency conflicts. 
You can use your favorite VE. Here we illustrate the setup using a Conda VE.

```
$ conda create --name eden
$ conda activate eden
```

## Install dependences:
```
$ conda install -c anaconda requests
$ conda install -c anaconda beautifulsoup4
$ conda install -c anaconda pandas
```

## Install the package:
```
$ pip install -e .
```

## Run a pipeline
```
$ python pipelines.py
```

---
### Copyright

Copyright © 2022, David W. Kastner

---
#### Acknowledgements
 
Project based on the MolSSi template v1.6.
