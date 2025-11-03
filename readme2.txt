改变块的大小 一个块8个页
写入25MB就可以有三层lsmtree
试试

在根目录下
编译：
javac -d classes src/com/ssd/*.java

运行：
#KVWorkloadRunner是只指定一个csv文件运行，直接输出到终端
java -cp classes com.ssd.KVWorkloadRunner csv2/kvssd.csv
#Runner是指定多个csv文件运行，输出到kv_workload_results目录下
java -cp classes com.ssd.Runner ./csv3/cache.csv ./csv3/cache15.csv ./csv3/crypto1.csv ./csv3/crypto2.csv ./csv3/dedup.csv ./csv3/etc.csv ./csv3/kvssd.csv ./csv3/rtdata.csv ./csv3/udb.csv ./csv3/var.csv ./csv3/w-pink.csv ./csv3/xbox.csv ./csv3/ycsb.csv ./csv3/zippydb.csv
java -cp classes com.ssd.Runner ./csv_new/cache.csv ./csv_new/cache15.csv ./csv_new/crypto1.csv ./csv_new/crypto2.csv ./csv_new/dedup.csv ./csv_new/etc.csv ./csv_new/kvssd.csv ./csv_new/rtdata.csv ./csv_new/udb.csv ./csv_new/var.csv ./csv_new/w-pink.csv ./csv_new/xbox.csv ./csv_new/ycsb.csv ./csv_new/zippydb.csv
KVSSD运行一次的数据持久化到PERSIST_DIR目录下，可在Constants.java中配置改目录

# 1. KVSSD
java -cp classes com.ssd.NewGenerator --output csv2/kvssd.csv --ops 12740 --numKeys 50000 --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform

# 2. YCSB
java -cp classes com.ssd.NewGenerator --output csv2/ycsb.csv --ops 51196 --numKeys 81920 --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform

# 3. W-PinK
java -cp classes com.ssd.NewGenerator --output csv2/w-pink.csv --ops 49460 --numKeys 79140 --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform

# 4. Xbox
java -cp classes com.ssd.NewGenerator --output csv2/xbox.csv --ops 40392 --numKeys 64630 --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform

# 5. ETC
java -cp classes com.ssd.NewGenerator --output csv2/etc.csv --ops 130096 --numKeys 208150 --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform

# 6. UDB
java -cp classes com.ssd.NewGenerator --output csv2/udb.csv --ops 329740 --numKeys 527590 --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform

# 7. Cache
java -cp classes com.ssd.NewGenerator --output csv2/cache.csv --ops 224054 --numKeys 358490 --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform

# 8. VAR
java -cp classes com.ssd.NewGenerator --output csv2/var.csv --ops 340446 --numKeys 544710 --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform

# 9. Crypto2
java -cp classes com.ssd.NewGenerator --output csv2/crypto2.csv --ops 347210 --numKeys 555540 --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

# 10. Dedup
java -cp classes com.ssd.NewGenerator --output csv2/dedup.csv --ops 771012 --numKeys 1233620 --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

# 11. Cache15
java -cp classes com.ssd.NewGenerator --output csv2/cache15.csv --ops 655360 --numKeys 1048580 --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform

# 12. ZippyDB
java -cp classes com.ssd.NewGenerator --output csv2/zippydb.csv --ops 551882 --numKeys 883010 --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

# 13. Crypto1
java -cp classes com.ssd.NewGenerator --output csv2/crypto1.csv --ops 403298 --numKeys 645280 --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform

# 14. RTDATA
java -cp classes com.ssd.NewGenerator --output csv2/rtdata.csv --ops 1379706 --numKeys 2207530 --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform


试试只有两层的吧