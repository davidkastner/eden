![Graphical Introduction to Eden](docs/_static/header.png)
Eden
==============================
[//]: # (Badges)
[![Documentation Status](https://readthedocs.org/projects/eden/badge/?version=latest)](https://eden.readthedocs.io/en/latest/?badge=latest)

---

# The Eden Project
## Table of Contents
1. **Introduction**
    * Introduction
    * Available Data
2. **Installation**
    * Dependencies
    * Installing the package
3. **What is included?**
    * Pipelines
4. **Documentation**
    * Read the Docs
    * Examples


## Introduction
Welcome to Eden! A data-oriented initiative to identify the perfect city. 
The project seeks to compile dozens of features for every city in the United States.
It is written to be 100% automated.

### Available Data
As new data is generated or updated it will be made available as interactive plots.
These are the data currently generated:

- [Population Density](https://eden.readthedocs.io/en/latest/_static/density.html)
- [Best Climate]()
- [Politics]()


## Installation
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

## Install dependences
```

$ conda install -c anaconda scikit-learn
$ conda install -c anaconda requests
$ conda install -c anaconda beautifulsoup4
$ conda install -c anaconda pandas
$ conda install -c plotly plotly_express
$ pip install sphinx sphinx_rtd_theme
$ conda install -c conda-forge sphinx-autoapi
$ pip install https://github.com/revitron/revitron-sphinx-theme/archive/master.zip
```

### Install the package:
```
$ pip install -e .
```

### Run the pipeline:
```
$ python pipelines.py
```

## Documentation
Documentation for Eden is a higher priority. Up to date documentation can be found at:
https://eden.readthedocs.io/en/latest/


---
### Copyright

Copyright © 2022, David W. Kastner

---
#### Acknowledgements
 
Project based on the MolSSi template v1.6.
