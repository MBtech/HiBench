#!/bin/bash

sed -i "s/hibench.hdfs.master.*/hibench.hdfs.master       hdfs:\/\/$(hostname --ip-address):9000/g" conf/hadoop.conf 

sed -i "s/hibench.spark.master.*/hibench.spark.master spark:\/\/$(hostname --ip-address):7077/g" conf/spark.conf

sizes=(tiny small large huge gigantic bigdata)
for size in ${sizes[@]}; do

sed -i "s/hibench.scale.profile.*/hibench.scale.profile $size/g" conf/hibench.conf
bin/run_all.sh
done
