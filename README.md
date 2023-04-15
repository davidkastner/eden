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
- [Elevation](https://eden.readthedocs.io/en/latest/_static/Elevation.html)
- [Best Climate](https://eden.readthedocs.io/en/latest/_static/ClimateScore.html)
- [Rainfall](https://eden.readthedocs.io/en/latest/_static/Rainfall.html)
- [Snowfall](https://eden.readthedocs.io/en/latest/_static/Snowfall.html)
- [Sunshine](https://eden.readthedocs.io/en/latest/_static/Sunshine.html)
- [UV Risk Map](https://eden.readthedocs.io/en/latest/_static/UV.html)
- [Days Above 90°](https://eden.readthedocs.io/en/latest/_static/Above90.html)
- [Days Below 30°](https://eden.readthedocs.io/en/latest/_static/Below30.html)
- [Days Below 0°](https://eden.readthedocs.io/en/latest/_static/Below0.html)
- [Water Purity](https://eden.readthedocs.io/en/latest/_static/WaterQuality.html)
- [Air Purity](https://eden.readthedocs.io/en/latest/_static/AirQuality.html)
- [Number of Physicians](https://eden.readthedocs.io/en/latest/_static/Physicians.html)
- [Healthcare Costs](https://eden.readthedocs.io/en/latest/_static/HealthCosts.html)
- [Constitutionality](https://eden.readthedocs.io/en/latest/_static/Constitutionality.html)
- [Democrat](https://eden.readthedocs.io/en/latest/_static/DemVotePred.html)
- [Republican](https://eden.readthedocs.io/en/latest/_static/RepVotePred.html)
- [Distance to Nearest Temple](https://eden.readthedocs.io/en/latest/_static/TempleDistance.html)
- [Home Insurance Costs](https://eden.readthedocs.io/en/latest/_static/HomeInsurance.html)
- [Drought Risk](https://eden.readthedocs.io/en/latest/_static/Drought.html)
- [Average Home Age](https://eden.readthedocs.io/en/latest/_static/MedianHomeAge.html)
- [Property Tax Rate](https://eden.readthedocs.io/en/latest/_static/PropertyTaxRate.html)
- [Median Home Cost](https://eden.readthedocs.io/en/latest/_static/MedianHomeCost.html)


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
