测试
1GB    zip
#1 KVSSD
java -cp classes com.myproject.utils.KVGeneratorString --output csv/kvssd.csv --ops 524288 --numKeys 50000 \
  --readRatio 0.5 --keySize 16 --valSize 4096 --keyDist zipf --zipfS 0.99
  Total Flash Reads (Times): 2,176,722
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 131,808
  1 time flash access: 158
  2 times flash access: 29,829
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 63,695


# 2. YCSB
java -cp classes com.myproject.utils.KVGeneratorString --output csv/ycsb.csv --ops 2097152 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 1000 --keyDist zipf --zipfS 0.99
Total Flash Reads (Times): 4,573,012
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 629,793
  1 time flash access: 96
  2 times flash access: 131,061
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 148,443


# 3. W-PinK
java -cp classes com.myproject.utils.KVGeneratorString --output csv/w-pink.csv --ops 2013200 --numKeys 50000 \
  --readRatio 0.5 --keySize 32 --valSize 1024 --keyDist zipf --zipfS 0.99
Total Flash Writes (Bytes): 610,533,376
Total Flash Reads (Times): 4,536,546
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 600,863
  1 time flash access: 90
  2 times flash access: 125,698
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 146,659
==================================================

# 4. Xbox
java -cp classes com.myproject.utils.KVGeneratorString --output csv/xbox.csv --ops 1638400 --numKeys 50000 \
  --readRatio 0.5 --keySize 94 --valSize 1200 --keyDist zipf --zipfS 0.99
Total Flash Writes (Bytes): 656,834,560
Total Flash Reads (Times): 4,385,508
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 471,705
  1 time flash access: 89
  2 times flash access: 102,634
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 134,802


# 5. ETC
java -cp classes com.myproject.utils.KVGeneratorString --output csv/etc.csv --ops 5380720 --numKeys 50000 \
  --readRatio 0.5 --keySize 41 --valSize 358 --keyDist zipf --zipfS 0.99
Total Flash Reads (Times): 4,563,692
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 1,917,286
  1 time flash access: 85
  2 times flash access: 326,579
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 147,983
==================================================


# 6. UDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv/udb.csv --ops 13762560 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128 --keyDist zipf --zipfS 0.99

# 7. Cache
java -cp classes com.myproject.utils.KVGeneratorString --output csv/cache.csv --ops 9322120 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188 --keyDist zipf --zipfS 0.99

# 8. VAR
java -cp classes com.myproject.utils.KVGeneratorString --output csv/var.csv --ops 14311552 --numKeys 50000 \
  --readRatio 0.5 --keySize 35 --valSize 115 --keyDist zipf --zipfS 0.99



# 9. Crypto2
  --readRatio 0.5 --keySize 37 --valSize 110 --keyDist zipf --zipfS 0.99

# 10. Dedup
java -cp classes com.myproject.utils.KVGeneratorString --output csv/dedup.csv --ops 33554432 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 44 --keyDist zipf --zipfS 0.99

# 11. Cache15
java -cp classes com.myproject.utils.KVGeneratorString --output csv/cache15.csv --ops 28101840 --numKeys 50000 \
  --readRatio 0.5 --keySize 38 --valSize 38 --keyDist zipf --zipfS 0.99

# 12. ZippyDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv/zippydb.csv --ops 23527296 --numKeys 50000 \
  --readRatio 0.5 --keySize 48 --valSize 43 --keyDist zipf --zipfS 0.99

# 13. Crypto1
java -cp classes com.myproject.utils.KVGeneratorString --output csv/crypto1.csv --ops 16777216 --numKeys 50000 \
  --readRatio 0.5 --keySize 76 --valSize 50 --keyDist zipf --zipfS 0.99

# 14. RTDATA
java -cp classes com.myproject.utils.KVGeneratorString --output csv/rtdata.csv --ops 62857120 --numKeys 50000 \
  --readRatio 0.5 --keySize 24 --valSize 10 --keyDist zipf --zipfS 0.99



  1GB/10   uniform   csv2
# 6. UDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/udb.csv --ops 1376256 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128 --keyDist uniform
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 233,166
  1 time flash access: 68
  2 times flash access: 256,595
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 2,909
========================================


# 7. Cache
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/cache.csv --ops 932212 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188 --keyDist uniform
Application Read Count: 465,572
GC Count: 0
Compaction Count: 28
Write Amplification: 0.85
Total Flash Writes (Bytes): 91,062,272
Total Flash Reads (Times): 1,470,907
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 108,426
  1 time flash access: 86
  2 times flash access: 140,587
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 16,916
==================================================


# 8. VAR
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/var.csv --ops 1431155 --numKeys 50000 \
  --readRatio 0.5 --keySize 35 --valSize 115 --keyDist uniform
Total Flash Writes (Bytes): 76,513,280
Total Flash Reads (Times): 1,455,438
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 250,278
  1 time flash access: 57
  2 times flash access: 269,847
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 2,483
==================================================


# 9. Crypto2
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/crypto2.csv --ops 1462016 --numKeys 50000 \
  --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform

# 10. Dedup
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/dedup.csv --ops 3355443 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 44 --keyDist uniform

# 11. Cache15
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/cache15.csv --ops 2810184 --numKeys 50000 \
  --readRatio 0.5 --keySize 38 --valSize 38 --keyDist uniform

# 12. ZippyDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/zippydb.csv --ops 2352729 --numKeys 50000 \
  --readRatio 0.5 --keySize 48 --valSize 43 --keyDist uniform

# 13. Crypto1
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/crypto1.csv --ops 1677721 --numKeys 50000 \
  --readRatio 0.5 --keySize 76 --valSize 50 --keyDist uniform

# 14. RTDATA
java -cp classes com.myproject.utils.KVGeneratorString --output csv2/rtdata.csv --ops 6285712 --numKeys 50000 \
  --readRatio 0.5 --keySize 24 --valSize 10 --keyDist uniform



  


  

  /10   uniform
# 6. UDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/udb.csv --ops 1376256 --numKeys 50000 \
  --readRatio 0.5 --keySize 27 --valSize 128  --keyDist uniform
Total Flash Reads (Times): 1,453,649
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 234,454
  1 time flash access: 71
  2 times flash access: 255,944
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 3,150
==================================================


# 7. Cache
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/cache.csv --ops 932212 --numKeys 50000 \
  --readRatio 0.5 --keySize 42 --valSize 188  --keyDist uniform
--------------------------------------------------
                 Detailed Stats (from KVSSD5)     
--------------------------------------------------
Application Write Count: 466,075
Application Read Count: 466,137
GC Count: 0
Compaction Count: 31
Write Amplification: 0.90
Total Flash Writes (Bytes): 96,305,152
Total Flash Reads (Times): 1,637,898
--------------------------------------------------
Read Flash Access Distribution:
  0 times flash access: 90,275
  1 time flash access: 0
  2 times flash access: 135,469
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 23,802
==================================================

# 8. VAR
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/var.csv --ops 1431155 --numKeys 50000 \
  --readRatio 0.5 --keySize 35 --valSize 115  --keyDist uniform



# 9. Crypto2
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/crypto2.csv --ops 1462016 --numKeys 50000 \
  --readRatio 0.5 --keySize 37 --valSize 110 --keyDist uniform
Read Flash Access Distribution:
  0 times flash access: 260,618
  1 time flash access: 70
  2 times flash access: 277,867
  3 times flash access: 0
  4 times flash access: 0
  5 time flash access: 0
  6 times flash access: 0
  7 times flash access: 0
  8 times flash access: 0
  8+ times flash access: 2,077
==================================================

# 10. Dedup
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/dedup.csv --ops 9355443 --numKeys 50000 \
  --readRatio 0.5 --keySize 20 --valSize 44  --keyDist uniform

# 11. Cache15
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/cache15.csv --ops 2810184 --numKeys 50000 \
  --readRatio 0.5 --keySize 38 --valSize 38  --keyDist uniform

# 12. ZippyDB
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/zippydb.csv --ops 2352729 --numKeys 50000 \
  --readRatio 0.5 --keySize 48 --valSize 43  --keyDist uniform

# 13. Crypto1
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/crypto1.csv --ops 1677721 --numKeys 50000 \
  --readRatio 0.5 --keySize 76 --valSize 50  --keyDist uniform

# 14. RTDATA
java -cp classes com.myproject.utils.KVGeneratorString --output csv4/rtdata.csv --ops 6285712 --numKeys 50000 \
  --readRatio 0.5 --keySize 24 --valSize 10  --keyDist uniform
