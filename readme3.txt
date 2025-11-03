
写入32MB就可以有两层lsmtree
一个SSTable为4MB
大概有两层

在根目录下
编译：
javac -d classes src/com/ssd/*.java

运行：
#KVWorkloadRunner是只指定一个csv文件运行，直接输出到终端
java -cp classes com.ssd.KVWorkloadRunner csv4/kvssd.csv
#Runner是指定多个csv文件运行，输出到kv_workload_results目录下
java -cp classes com.ssd.Runner ./csv3/cache.csv ./csv3/cache15.csv ./csv3/crypto1.csv ./csv3/crypto2.csv ./csv3/dedup.csv ./csv3/etc.csv ./csv3/kvssd.csv ./csv3/rtdata.csv ./csv3/udb.csv ./csv3/var.csv ./csv3/w-pink.csv ./csv3/xbox.csv ./csv3/ycsb.csv ./csv3/zippydb.csv
KVSSD运行一次的数据持久化到PERSIST_DIR目录下，可在Constants.java中配置改目录



试试只有两层的吧
## 1. KVSSD
 java -cp classes com.ssd.NewGenerator --output csv4/kvssd.csv --ops 16304 --numKeys 50000 --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist uniform

 # 2. YCSB
 java -cp classes com.ssd.NewGenerator --output csv4/ycsb.csv --ops 65536 --numKeys 81920 --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist uniform

 # 3. W-PinK
 java -cp classes com.ssd.NewGenerator --output csv4/w-pink.csv --ops 63312 --numKeys 79140 --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist uniform

 # 4. Xbox
 java -cp classes com.ssd.NewGenerator --output csv4/xbox.csv --ops 51704 --numKeys 64630 --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist uniform

 # 5. ETC
 java -cp classes com.ssd.NewGenerator --output csv4/etc.csv --ops 166524 --numKeys 208150 --readRatio 0.5 --keySize 41 --valSize 358 --keyDist uniform

 # 6. UDB
 java -cp classes com.ssd.NewGenerator --output csv4/udb.csv --ops 422070 --numKeys 527590 --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform

 # 7. Cache
 java -cp classes com.ssd.NewGenerator --output csv4/cache.csv --ops 286800 --numKeys 358490 --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform

 # 8. VAR
 java -cp classes com.ssd.NewGenerator --output csv4/var.csv --ops 435772 --numKeys 544710 --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform

 # 9. Crypto2
 java -cp classes com.ssd.NewGenerator --output csv4/crypto2.csv --ops 444430 --numKeys 555540 --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

 # 10. Dedup
 java -cp classes com.ssd.NewGenerator --output csv4/dedup.csv --ops 986896 --numKeys 1233620 --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

 # 11. Cache15
 java -cp classes com.ssd.NewGenerator --output csv4/cache15.csv --ops 838860 --numKeys 1048580 --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform

 # 12. ZippyDB
 java -cp classes com.ssd.NewGenerator --output csv4/zippydb.csv --ops 706410 --numKeys 883010 --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

 # 13. Crypto1
 java -cp classes com.ssd.NewGenerator --output csv4/crypto1.csv --ops 516222 --numKeys 645280 --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform

 # 14. RTDATA
 java -cp classes com.ssd.NewGenerator --output csv4/rtdata.csv --ops 1766024 --numKeys 2207530 --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform