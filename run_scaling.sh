#!/bin/bash

sizes=(tiny small large huge gigantic bigdata)
for size in ${sizes[@]}; do

sed -i "s/hibench.scale.profile.*/hibench.scale.profile $size/g" conf/hibench.conf
bin/run_all.sh
done
