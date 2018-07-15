# Scalable.OR (Scalable OpenRefine)

OpenRefine (http://openrefine.org/) is an application for working with raw data. The data can be imported and manipulated (cleaning, transformation, etc.), and exported using built-in Web interface of OpenRefine. Furthermore, the sequence of manipulation operations can be exported using Web UI in JSON format.

The storing of data in OpenRefine is realized inefficiently, since the whole data is loaded at once into memory. In other words, it means that maximal size of processed data is limited by RAM memory on the PC. Thus, it is needed to create a new interpreter application such as 'Scalable.OR', which executes JSON operation sequence from OpenRefine, using Spark Framework. Spark provides an interface to work with data on computing cluster, and allows efficient processing of big data on one host PC too. 

For this purpose, the Scalable.OR interpreter has to implement base functionality of OpenRefine project, including evaluation of GREL (General Refine Expression Language) and Python expressions with functional environment similar to OpenRefine.

## Usage
```
python start.py [-h] [-m MASTER] [-i INPUT] [-p OR_PROGRAM] [-o OUTPUT] [-l]

Required parameters:
-i, --input         - set path to input file
-p, --or-program    - set path to json file with OR program
-o, --output        - set path to output file

Optional parameters (defaults can be changed in config.ini):
-m, --master           - set spark master                         (default: spark://localhost:7077)
-l, --in-proc          - use in-proc spark instance               (default: False)
-v, --verbose          - increase output verbosity                (default: False)
--spark-home           - set path to spark                        (default: /usr/local/spark)
--csv-sep              - set CSV separator (delimiter)            (default: ',')
--col-names-first-row  - columns are named after first row (CSV)  (default: False)
```

### How to start from command line

Please set required environment variables:
```
#!bash
export SPARK_HOME=/usr/local/spark
export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/lib/py4j-0.9-src.zip:$SPARK_HOME/python/lib/pyspark.zip
```

Example: run Scalable.OR
```
#!bash
python scalable.or/start.py -i scalable.or/tests/integration/core-text-transform/input.csv -p scalable.or/tests/integration/core-text-transform/or.json -o output.csv -l
```

Example: run unittest
```
#!bash
python -m unittest discover -s scalable.or/tests/
```

### Configuring Scalable.OR
Scalable.OR requires a config.ini. Just rename config.default.ini to config.ini (or copy in order to keep the original file) and adjust the parameters.

## How to extend Scalable.OR

### Add a new method to Scalable.OR project

Required parameters:
    - cmd: OpenRefine command object
    - df:  Spark DataFrame object
    - rdd: Spark Resilient Distributed Dataset object
    - sc:  Spark context


Example:
```
#!python
# Register a new method in Scalable.OR by name
@MethodsManager.register("spark/do-something")
def method_name_is_not_relevant(cmd, df, rdd, sc):
    # do something with data
    new_dataframe = df.withColumnRenamed(cmd["oldColumnName"], cmd["newColumnName"])

    # return new dataframe with manipulated data
    # if data isn't changed please return original dataframe
    return new_dataframe
```

### Create a Scalable.OR start program with new methods

```
#!python
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import scalableor
from scalableor.method import MethodsManager

@MethodsManager.register("temp-project/method-1")
def method_name(cmd, df, rdd, sc):
    return df

@MethodsManager.register("temp-project/method-2")
def method_name(cmd, df, rdd, sc):
    return df

@MethodsManager.register("temp-project/method-3")
def method_name(cmd, df, rdd, sc):
    return df

if __name__ == "__main__":
    scalableor.run()

```

### Add external JAR

Used external libraries are stored in 'vendor/jar' directory.
Scalable.OR includes jar files from this directory on program start.


## Tested platform

* Ubuntu 14.04
    - OpenJDK 1.7.0_111
    - Scala 2.10.4
    - Spark 1.6.1
    - Python 2.7.6


* Ubuntu 15.10
    - OpenJDK 1.8.0_91
    - Scala 2.11.6
    - Spark 1.6.1
    - Python 2.7.10

## Additional information

### Using PyCharm IDE

1. add required python packages to project structure:
    - py4j      ($SPARK_HOME/python/lib/py4j-0.9-src.zip)
    - pyspark   ($SPARK_HOME/python/lib/pyspark.zip)

### Code coverage (required coverage library)

```
# install coverage library
pip install coverage
# run tests
coverage run -m unittest discover -s scalable.or/tests/
# get coverage report
coverage report -m --include *scalableor*
```

### Spark installation

* Install Java

```
#!bash
# installation of Oracle Java JDK.
sudo apt-get -y update
sudo apt-get -y install python-software-properties
sudo add-apt-repository -y ppa:webupd8team/java
sudo apt-get -y update
sudo apt-get -y install oracle-java7-installer
```

* Install Scala

```
#!bash
# scala 2.10
wget www.scala-lang.org/files/archive/scala-2.10.4.deb
sudo dpkg -i scala-2.10.4.deb
# scala 2.11
wget www.scala-lang.org/files/archive/scala-2.11.6.deb
sudo dpkg -i scala-2.11.6.deb
```

* Install Spark (http://spark.apache.org/downloads.html)
```
#!bash
wget http://d3kbcqa49mib13.cloudfront.net/spark-1.6.1-bin-hadoop2.6.tgz
tar xvf spark-1.6.1-bin-hadoop2.6.tgz
sudo mv spark-1.6.1-bin-hadoop2.6 /usr/local/spark
```

* Extend ENV
```
#!bash
export SPARK_HOME=/usr/local/spark
```

* Start/Stop Spark instances
```
#!bash
# start
sudo /usr/local/spark/sbin/start-master.sh --ip localhost
sudo /usr/local/spark/sbin/start-slave.sh spark://localhost:7077
# stop
sudo /usr/local/spark/sbin/stop-slave.sh
sudo /usr/local/spark/sbin/stop-master.sh
```

### Useful links:

* OpenRefine:
    - homepage: http://openrefine.org/
    - download: http://openrefine.org/download.html
    - git repository: https://github.com/OpenRefine/OpenRefine
    - documentation: https://github.com/OpenRefine/OpenRefine/wiki
        * installation: https://github.com/OpenRefine/OpenRefine/wiki/Installation-Instructions
        * GREL references: https://github.com/OpenRefine/OpenRefine/wiki/Documentation-For-Users#reference

* Spark:
    - homepage: http://spark.apache.org/
    - documentation: https://spark.apache.org/docs/1.6.1/
    - download: http://spark.apache.org/downloads.html