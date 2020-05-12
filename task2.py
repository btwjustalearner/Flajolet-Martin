import os
from pyspark import SparkContext
import sys
import json
import time
from pyspark.streaming import StreamingContext
import binascii
import random
import statistics

# spark-submit task2.py <port#> <output_file_path>
# spark-submit task2.py 9999 task2.csv

# java -cp <generate_stream.jar file path> StreamSimulation <business.json file path> 9999 100
# java -cp generate_stream.jar StreamSimulation business.json 9999 100

start_time = time.time()
portnumber = int(sys.argv[1])
output_file_path = sys.argv[2]

output = open(output_file_path, "w")
output.write('Time,Ground Truth,Estimation')
output.write('\n')
output.close()

prime_numbers = [77743, 92479, 117373, 127081, 139487, 154127, 170141, 194933, 213859, 214433, 228023, 230239, 228539,
                 470551]

m = 29473


def num_trailing_0(s):
    return len(s) - len(s.rstrip('0'))


def f_mFunc(rdd):
    t = time.localtime()
    tm = '%d-%02d-%02d %02d:%02d:%02d' % (t.tm_year, t.tm_mon, t.tm_mday, t.tm_hour, t.tm_min, t.tm_sec)
    data = rdd.map(lambda x: json.loads(x)['city'])\
        .filter(lambda x: len(x) > 0)\
        .collect()
    ground_truth = len(set(data))
    hash_num = 10000
    group_num = 1000
    hashpergroup = int(hash_num/group_num)
    groupavglist = []
    for i in range(group_num):
        grouprlist = []
        for j in range(hashpergroup):
            int1 = random.randint(1, 10000)
            int2 = random.randint(1, 10000)
            def getcity2r(c):
                cityint = int(binascii.hexlify(c.encode('utf8')), 16)
                cityinthash = (cityint * int1 + int2) % m
                citybin = format(cityinthash, 'b')
                num0 = num_trailing_0(citybin)
                return 2**num0

            rlist = [getcity2r(city) for city in data]
            bigr = max(rlist)
            grouprlist.append(bigr)
        groupavg = statistics.median(grouprlist)
        groupavglist.append(groupavg)
    estimation = int(statistics.median(groupavglist))
    f = open(output_file_path, 'a')
    f.write(str(tm) + "," + str(ground_truth) + "," + str(estimation) + "\n")
    f.close()


sc = SparkContext('local[*]', 'task2')
sc.setLogLevel('OFF')

ssc = StreamingContext(sc, 5)
lines = ssc.socketTextStream("localhost", portnumber).window(30, 10)

lines.foreachRDD(f_mFunc)

ssc.start()
ssc.awaitTermination()

# print('Duration: ' + str(time.time() - start_time))