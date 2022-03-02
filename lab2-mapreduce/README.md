# MapReduce labs

# What is this
This repository is a series of labs designed to help Big Data Students understand and practice each components of the hadoop echosystem.

The repository will be updated regularly during the whole semester.

Feel free to open an issue or a pull-request if you have any suggestion

# Prerequisites
* Linux
* docker

# Getting started
## first run

Start by cloning this repository on your Linux machine
```
git clone https://github.com/houssem-tebai/bigdata-labs.git
```
run the hadoop cluster
```
docker run -itd --net=hadoop -p 50070:50070 -p 8088:8088 -p 7077:7077 -p 16010:16010 \
            --name hadoop-master --hostname hadoop-master \
            houssemtebai/hadoop:2.7.3

  docker run -itd -p 8040:8042 --net=hadoop \
        --name hadoop-slave1 --hostname hadoop-slave1 \
              houssemtebai/hadoop:2.7.3

  docker run -itd -p 8041:8042 --net=hadoop \
        --name hadoop-slave2 --hostname hadoop-slave2 \
              houssemtebai/hadoop:2.7.3
```

Check your environment by running 
```
docker ps
```
## Start Hadoop Cluster

to start you cluster after a machine reboot run the folowing command
```
docker start hadoop-master hadoop-slave1 hadoop-slave2
```
to execute commands on the master node
```
docker exec -it hadoop-master bash

./start-hadoop.sh
```
