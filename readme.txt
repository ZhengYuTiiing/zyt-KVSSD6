这个是用已经实现remapping compaction的,但是这个remap有点问题，没有实现共享的重叠页（shared overlap page）

项目运行手册
再根目录下
编译：
javac -d classes src/com/myproject/utils/*.java

运行：
#KVWorkloadRunner是只指定一个csv文件运行，直接输出到终端
java -cp classes com.myproject.utils.KVWorkloadRunner workloads2/cache_large.csv
#Runner是指定多个csv文件运行，输出到kv_workload_results目录下
java -cp classes com.ssd.Runner ./csv3/cache15.csv ./csv3/zippydb.csv ./csv3/crypto1.csv ./csv3/rtdata.csv

KVSSD运行一次的数据持久化到PERSIST_DIR目录下，可在Constants.java中配置改目录

使用下面的命令来生成csv文件，--output参数后指定要生成到哪个文件里
java -cp classes com.myproject.utils.KVGeneratorString --output csv/kvssd.csv --ops 1000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform


在flashaccess目录下
编译： javac -d classes src/com/myproject/utils/*.java
运行： java -cp classes com.myproject.utils.KVWorkloadRunner workloads2/cache_large.csv 
运行的时候记得指定csv文件。


测试的数据（csv文件）放在/csv目录下

# 1. KVSSD
java -cp classes com.myproject.utils.KVGeneratorString --output csv/kvssd.csv --ops 1000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform

# 2. YCSB
java -cp classes com.myproject.utils.KVGeneratorString --output csv/ycsb.csv --ops 1000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform

# 3. W-PinK
java -cp classes com.myproject.utils.KVGeneratorString --output csv/w-pink.csv --ops 1000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform

# 4. Xbox
java -cp classes com.myproject.utils.KVGeneratorString --output csv/xbox.csv --ops 1000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform

# 5. ETC
java -cp classes com.myproject.utils.KVGeneratorString --output csv/etc.csv --ops 5000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform

# 6. UDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv/udb.csv --ops 5000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform

# 7. Cache
java -cp classes com.myproject.utils.KVGeneratorString --output csv/cache.csv --ops 5000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform

# 8. VAR
java -cp classes com.myproject.utils.KVGeneratorString --output csv/var.csv --ops 5000000 --numKeys 50000 \
  --readRatio 0.5 --keySize  --35valSize 115 --keyDist uniform

# 9. Crypto2
java -cp classes com.myproject.utils.KVGeneratorString --output csv/crypto2.csv --ops 5000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

# 10. Dedup
java -cp classes com.myproject.utils.KVGeneratorString --output csv/dedup.csv --ops 10000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

# 11. Cache15
java -cp classes com.myproject.utils.KVGeneratorString --output csv/cache15.csv --ops 10000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform


# 12. ZippyDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv/zippydb.csv --ops 10000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

# 13. Crypto1
java -cp classes com.myproject.utils.KVGeneratorString --output csv/crypto1.csv --ops 8000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform


# 14. RTDATA
java -cp classes com.myproject.utils.KVGeneratorString --output csv/rtdata.csv --ops 8000000 --numKeys 50000 \
  --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform




4GB
# 1. KVSSD：Key=16B + Value=4096B = 4112B/写KV → 总操作数=4GB/(4112B×0.5)≈2,097,152
java -cp classes com.myproject.utils.KVGeneratorString --output csv/kvssd.csv --ops 2097152 --numKeys 50000 \
  --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform

# 2. YCSB：Key=20B + Value=1000B = 1020B/写KV → 总操作数=4GB/(1020B×0.5)≈8,388,608
java -cp classes com.myproject.utils.KVGeneratorString --output csv/ycsb.csv --ops 8388608 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform

# 3. W-PinK：Key=32B + Value=1024B = 1056B/写KV → 总操作数=4GB/(1056B×0.5)≈8,053,063（取整8053000）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/w-pink.csv --ops 8053000 --numKeys 50000 \
  --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform

# 4. Xbox：Key=94B + Value=1200B = 1294B/写KV → 总操作数=4GB/(1294B×0.5)≈6,553,600（取整6553600）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/xbox.csv --ops 6553600 --numKeys 50000 \
  --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform

# 5. ETC：Key=41B + Value=358B = 399B/写KV → 总操作数=4GB/(399B×0.5)≈21,522,880（取整21522880）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/etc.csv --ops 21522880 --numKeys 50000 \
  --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform

# 6. UDB：Key=27B + Value=128B = 155B/写KV → 总操作数=4GB/(155B×0.5)≈55,050,240（取整55050240）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/udb.csv --ops 55050240 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform

# 7. Cache：Key=42B + Value=188B = 230B/写KV → 总操作数=4GB/(230B×0.5)≈37,288,480（取整37288480）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/cache.csv --ops 37288480 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform

# 8. VAR：Key=35B + Value=115B = 150B/写KV → 总操作数=4GB/(150B×0.5)≈57,266,176（取整57266176）（修正原参数格式）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/var.csv --ops 57266176 --numKeys 50000 \
  --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform

# 9. Crypto2：Key=37B + Value=110B = 147B/写KV → 总操作数=4GB/(147B×0.5)≈58,480,640（取整58480640）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/crypto2.csv --ops 58480640 --numKeys 50000 \
  --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

# 10. Dedup：Key=20B + Value=44B = 64B/写KV → 总操作数=4GB/(64B×0.5)=134,217,728（精确值）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/dedup.csv --ops 134217728 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

# 11. Cache15：Key=38B + Value=38B = 76B/写KV → 总操作数=4GB/(76B×0.5)≈112,407,360（取整112407360）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/cache15.csv --ops 112407360 --numKeys 50000 \
  --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform

# 12. ZippyDB：Key=48B + Value=43B = 91B/写KV → 总操作数=4GB/(91B×0.5)≈94,109,184（取整94109184）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/zippydb.csv --ops 94109184 --numKeys 50000 \
  --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

# 13. Crypto1：Key=76B + Value=50B = 126B/写KV → 总操作数=4GB/(126B×0.5)≈67,108,864（取整67108864）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/crypto1.csv --ops 67108864 --numKeys 50000 \
  --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform

# 14. RTDATA：Key=24B + Value=10B = 34B/写KV → 总操作数=4GB/(34B×0.5)≈251,428,480（取整251428480）
java -cp classes com.myproject.utils.KVGeneratorString --output csv/rtdata.csv --ops 251428480 --numKeys 50000 \
  --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform



1GB
# 1. KVSSD：Key=16B + Value=4096B=4112B/写KV → 总操作数=1GB/(4112B×0.5)≈524,288
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/kvssd.csv --ops 524288 --numKeys 50000 \
  --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform

结果：
Total Flash Reads (Times): 7,282,442
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 29,037
  1 time flash access: 226
  2 times flash access: 6,554
  3 times flash access: 0
  4 times flash access: 0
  5+ times flash access: 217,704
==================================================

# 2. YCSB：Key=20B + Value=1000B=1020B/写KV → 总操作数=1GB/(1020B×0.5)≈2,097,152
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/ycsb.csv --ops 2097152 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform

java -cp classes com.myproject.utils.KVGeneratorString --output csv2/ycsb2.csv --ops 1007152 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform
Total Flash Reads (Times): 30,100,156
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 4,283
  1 time flash access: 134
  2 times flash access: 4,369
  3 times flash access: 0
  4 times flash access: 0
  5+ times flash access: 490,013
==================================================


# 3. W-PinK：Key=32B + Value=1024B=1056B/写KV → 总操作数=1GB/(1056B×0.5)≈2,013,266（取整2013200）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/w-pink.csv --ops 2013200 --numKeys 50000 \
  --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform

java -cp classes com.myproject.utils.KVGeneratorString --output csv2/w-pink.csv --ops 513200 --numKeys 50000 \
  --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform
Total Flash Reads (Times): 2,922,835
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 14,100
  1 time flash access: 106
  2 times flash access: 22,679
  3 times flash access: 0
  4 times flash access: 0
  5+ times flash access: 197,902

# 4. Xbox：Key=94B + Value=1200B=1294B/写KV → 总操作数=1GB/(1294B×0.5)≈1,638,400（取整1638400）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/xbox.csv --ops 1638400 --numKeys 50000 \
  --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform

/8
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/xbox2.csv --ops 238400 --numKeys 50000 \
  --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform
Total Flash Writes (Bytes): 168,230,912
Total Flash Reads (Times): 1,262,130
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 7,299
  1 time flash access: 180
  2 times flash access: 10,307
  3 times flash access: 0
  4 times flash access: 0
  5+ times flash access: 90,905


# 5. ETC：Key=41B + Value=358B=399B/写KV → 总操作数=1GB/(399B×0.5)≈5,380,720（取整5380720）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/etc.csv --ops 5380720 --numKeys 50000 \
  --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform
/10
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/etc2.csv --ops 538072 --numKeys 50000 \
  --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform
Write Amplification: 0.94
Total Flash Writes (Bytes): 100,925,440
Total Flash Reads (Times): 1,312,786
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 39,663
  1 time flash access: 86
  2 times flash access: 56,132
  3 times flash access: 0
  4 times flash access: 0
  5+ times flash access: 130,224
==================================================


# 6. UDB：Key=27B + Value=128B=155B/写KV → 总操作数=1GB/(155B×0.5)≈13,762,560（取整13762560）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/udb.csv --ops 13762560 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform
  /50
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/udb2.csv --ops 275251 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform
Total Flash Writes (Bytes): 12,779,520
Total Flash Reads (Times): 187,124
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 65,987
  1 time flash access: 46
  2 times flash access: 50,775
  3 times flash access: 0
  4 times flash access: 0
  5+ times flash access: 3,843


# 7. Cache：Key=42B + Value=188B=230B/写KV → 总操作数=1GB/(230B×0.5)≈9,322,120（取整9322120）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/cache.csv --ops 9322120 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform
/60
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/cache2.csv --ops 155368 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform


# 8. VAR：Key=35B + Value=115B=150B/写KV → 总操作数=1GB/(150B×0.5)≈14,311,552（取整14311552）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/var.csv --ops 14311552 --numKeys 50000 \
  --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform
/10
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/var2.csv --ops 1431155 --numKeys 50000 \
  --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 250,045
  1 time flash access: 65
  2 times flash access: 270,091
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 2,306
  
# 9. Crypto2：Key=37B + Value=110B=147B/写KV → 总操作数=1GB/(147B×0.5)≈14,620,160（取整14620160）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/crypto2.csv --ops 14620160 --numKeys 50000 \
  --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

# 10. Dedup：Key=20B + Value=44B=64B/写KV → 总操作数=1GB/(64B×0.5)=33,554,432（精确值）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/dedup.csv --ops 33554432 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

# 11. Cache15：Key=38B + Value=38B=76B/写KV → 总操作数=1GB/(76B×0.5)≈28,101,840（取整28101840）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/cache15.csv --ops 28101840 --numKeys 50000 \
  --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform

# 12. ZippyDB：Key=48B + Value=43B=91B/写KV → 总操作数=1GB/(91B×0.5)≈23,527,296（取整23527296）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/zippydb.csv --ops 23527296 --numKeys 50000 \
  --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

# 13. Crypto1：Key=76B + Value=50B=126B/写KV → 总操作数=1GB/(126B×0.5)≈16,777,216（取整16777216）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/crypto1.csv --ops 16777216 --numKeys 50000 \
  --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform

# 14. RTDATA：Key=24B + Value=10B=34B/写KV → 总操作数=1GB/(34B×0.5)≈62,857,120（取整62857120）
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/rtdata.csv --ops 62857120 --numKeys 50000 \
  --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform



1GB
# 1. KVSSD
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/kvssd.csv --ops 524288 --numKeys 50000 \
  --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform

# 2. YCSB
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/ycsb.csv --ops 2097152 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform

# 3. W-PinK
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/w-pink.csv --ops 2013200 --numKeys 50000 \
  --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform

# 4. Xbox
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/xbox.csv --ops 1638400 --numKeys 50000 \
  --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform

# 5. ETC
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/etc.csv --ops 5380720 --numKeys 50000 \
  --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform

# 6. UDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/udb.csv --ops 13762560 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform

# 7. Cache
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/cache.csv --ops 9322120 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform

# 8. VAR
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/var.csv --ops 14311552 --numKeys 50000 \
  --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform

# 9. Crypto2
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/crypto2.csv --ops 14620160 --numKeys 50000 \
  --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

# 10. Dedup
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/dedup.csv --ops 33554432 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

# 11. Cache15
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/cache15.csv --ops 28101840 --numKeys 50000 \
  --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform

# 12. ZippyDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/zippydb.csv --ops 23527296 --numKeys 50000 \
  --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

# 13. Crypto1
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/crypto1.csv --ops 16777216 --numKeys 50000 \
  --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform

# 14. RTDATA
java -cp classes com.myproject.utils.KVGeneratorString --output csv3/rtdata.csv --ops 62857120 --numKeys 50000 \
  --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform



运行的data存在/data_1GB目录下



