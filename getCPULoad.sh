#!/bin/sh

# getCPULoad.sh
# pbsTools
#
# Created by nicain on 9/8/10.
# Copyright 2010 University of Washington. All rights reserved.

totalUsage=`ps aux|awk 'NR > 0 { s +=$3 }; END {print s}'`
nCPUs=`cat /proc/cpuinfo | grep processor | wc -l`
result=`echo "$totalUsage / $nCPUs " | bc -l`

echo $result