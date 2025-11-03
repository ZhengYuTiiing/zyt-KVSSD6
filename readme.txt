这个是试图实现共享的重叠页（shared overlap page）

项目运行手册
在根目录下
编译：
javac -d classes src/com/ssd/*.java

运行：
#KVWorkloadRunner是只指定一个csv文件运行，直接输出到终端
java -cp classes com.myproject.utils.KVWorkloadRunner workloads2/cache_large.csv
#Runner是指定多个csv文件运行，输出到kv_workload_results目录下
java -cp classes com.ssd.Runner ./csv3/cache.csv ./csv3/cache15.csv ./csv3/crypto1.csv ./csv3/crypto2.csv ./csv3/dedup.csv ./csv3/etc.csv ./csv3/kvssd.csv ./csv3/rtdata.csv ./csv3/udb.csv ./csv3/var.csv ./csv3/w-pink.csv ./csv3/xbox.csv ./csv3/ycsb.csv ./csv3/zippydb.csv
java -cp classes com.ssd.Runner ./csv_new/cache.csv ./csv_new/cache15.csv ./csv_new/crypto1.csv ./csv_new/crypto2.csv ./csv_new/dedup.csv ./csv_new/etc.csv ./csv_new/kvssd.csv ./csv_new/rtdata.csv ./csv_new/udb.csv ./csv_new/var.csv ./csv_new/w-pink.csv ./csv_new/xbox.csv ./csv_new/ycsb.csv ./csv_new/zippydb.csv
KVSSD运行一次的数据持久化到PERSIST_DIR目录下，可在Constants.java中配置改目录

使用下面的命令来生成csv文件，--output参数后指定要生成到哪个文件里
java -cp classes com.myproject.utils.KVGeneratorString --output csv/kvssd.csv --ops 1000000 --numKeys 50000 --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform
#需要提前创建好csv目录


NewGenerator是新的生成器，读的key都是写过的
使用下面的命令来生成csv文件，--output参数后指定要生成到哪个文件里
javac com/ssd/NewGenerator.java
java -cp classes com.ssd.NewGenerator --output csv_new/kvssd.csv --ops 1000000 --numKeys 50000 --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform
#需要提前创建好csv目录
# 1. KVSSD
# 单条写操作字节数=4+16+4096=4116，需写操作≈101899，总操作≈203798
java -cp classes com.ssd.NewGenerator --output csv_new/kvssd.csv --ops 20380 --numKeys 50000 --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform

# 2. YCSB
# 单条写操作字节数=4+20+1000=1024，需写操作=409600，总操作=819200
java -cp classes com.ssd.NewGenerator --output csv_new/ycsb.csv --ops 81920 --numKeys 81920 --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform

# 3. W-PinK
# 单条写操作字节数=4+32+1024=1060，需写操作≈395689，总操作≈791378
java -cp classes com.ssd.NewGenerator --output csv_new/w-pink.csv --ops 79140 --numKeys 79140 --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform

# 4. Xbox
# 单条写操作字节数=4+94+1200=1298，需写操作≈323135，总操作≈646270
java -cp classes com.ssd.NewGenerator --output csv_new/xbox.csv --ops 64630 --numKeys 64630 --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform

# 5. ETC
# 单条写操作字节数=4+41+358=403，需写操作≈1040770，总操作≈2081540
java -cp classes com.ssd.NewGenerator --output csv_new/etc.csv --ops 208150 --numKeys 208150 --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform

# 6. UDB
# 单条写操作字节数=4+27+128=159，需写操作≈2637927，总操作≈5275854
java -cp classes com.ssd.NewGenerator --output csv_new/udb.csv --ops 527590 --numKeys 527590 --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform

# 7. Cache
# 单条写操作字节数=4+42+188=234，需写操作≈1792437，总操作≈3584874
java -cp classes com.ssd.NewGenerator --output csv_new/cache.csv --ops 358490 --numKeys 358490 --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform

# 8. VAR（修正原指令的keySize参数错误）
# 单条写操作字节数=4+35+115=154，需写操作≈2723574，总操作≈5447148
java -cp classes com.ssd.NewGenerator --output csv_new/var.csv --ops 544710 --numKeys 544710 --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform

# 9. Crypto2
# 单条写操作字节数=4+37+110=151，需写操作≈2777684，总操作≈5555368
java -cp classes com.ssd.NewGenerator --output csv_new/crypto2.csv --ops 555540 --numKeys 555540 --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

# 10. Dedup
# 单条写操作字节数=4+20+44=68，需写操作≈6168094，总操作≈12336188
java -cp classes com.ssd.NewGenerator --output csv_new/dedup.csv --ops 1233620 --numKeys 1233620 --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

# 11. Cache15
# 单条写操作字节数=4+38+38=80，需写操作=5242880，总操作=10485760
java -cp classes com.ssd.NewGenerator --output csv_new/cache15.csv --ops 1048580 --numKeys 1048580 --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform

# 12. ZippyDB
# 单条写操作字节数=4+48+43=95，需写操作≈4415057，总操作≈8830114
java -cp classes com.ssd.NewGenerator --output csv_new/zippydb.csv --ops 883010 --numKeys 883010 --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

# 13. Crypto1
# 单条写操作字节数=4+76+50=130，需写操作≈3226388，总操作≈6452776
java -cp classes com.ssd.NewGenerator --output csv_new/crypto1.csv --ops 645280 --numKeys 645280 --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform

# 14. RTDATA
# 单条写操作字节数=4+24+10=38，需写操作≈11037642，总操作≈22075284
java -cp classes com.ssd.NewGenerator --output csv_new/rtdata.csv --ops 2207530 --numKeys 2207530 --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform


在flashaccess目录下
编译： javac -d classes src/com/myproject/utils/*.java
运行： java -cp classes com.myproject.utils.KVWorkloadRunner workloads2/cache_large.csv 
运行的时候记得指定csv文件。


测试的数据（csv文件）放在/csv目录下


















