package com.ssd;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class KVWorkloadRunner {
    public static void main(String[] args) {
        // 1. 参数校验：确保传入 CSV 路径
        if (args.length < 1) {
            System.out.println("Usage: java KVWorkloadRunner <workload.csv>");
            System.out.println("Example: java KVWorkloadRunner ycsb.csv");
            return;
        }
        String workloadFile = args[0];

        // 2. 初始化模拟器和统计变量
        KVSSD6 kvssd = new KVSSD6(); // 实例化 KVSSD5 模拟器
        long totalOps = 0;
        long readOps = 0, writeOps = 0;

        System.out.println("=== Running workload: " + workloadFile + " ===");
        long startTime = System.nanoTime();

        // 3. CSV 读取逻辑（直接处理明文字符串 Key/Value，无解码）
        BufferedReader br = null;
        try {
            br = new BufferedReader(new FileReader(workloadFile));
            
            // 跳过表头（CSV 表头格式应为：op,key_str,value_str，与 KVGenerator 输出匹配）
            String line = br.readLine();
            if (line == null) {
                System.err.println("Error: CSV 文件为空！");
                return;
            }
            System.out.println("Skipped CSV header: " + line);

            // 循环读取每一行数据
            while ((line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue; // 跳过空行

                // 分割 CSV 行：按逗号分割（最多3段：op, 明文字符串Key, 明文字符串Value）
                // 注意：若Value包含逗号，需用引号包裹（标准CSV格式），此处按基础场景处理
                String[] parts = line.split(",", 3);
                if (parts.length < 2) { // 至少需要 "操作类型(op)" 和 "明文字符串Key"
                    System.err.println("Skipped invalid line (格式错误): " + line);
                    continue;
                }

                // 直接读取明文字符串（核心修改：移除 hex 和 Base64 解码）
                String op = parts[0].trim();       // 操作类型（R/W）
                String key = parts[1].trim();      // 明文字符串Key（无需解码）
                String value = parts.length >= 3 ? parts[2].trim() : ""; // 明文字符串Value（无需解码）

                // 执行读/写操作
                if ("W".equals(op)) { // 写操作：直接用明文字符串 Key/Value
                    kvssd.put(key, value);
                    writeOps++;
                } else if ("R".equals(op)) { // 读操作：直接用明文字符串 Key
                    kvssd.get(key);
                    readOps++;
                } else { // 未知操作类型，跳过
                    System.err.println("Skipped unknown op (" + op + ") in line: " + line);
                    continue;
                }

                // 进度打印（每10000次操作打印一次）
                totalOps++;
                if (totalOps % 10000 == 0) {
                    System.out.println("Processed " + totalOps + " ops...");
                }
            }

        } catch (IOException e) { // 捕获CSV读取异常
            System.err.println("Error reading CSV file: " + e.getMessage());
            e.printStackTrace();
        } finally { // 确保流关闭，避免资源泄漏
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    System.err.println("Error closing CSV reader: " + e.getMessage());
                }
            }
        }

        // 4. 计算性能指标
        long endTime = System.nanoTime();
        double seconds = (endTime - startTime) / 1e9; // 纳秒转秒
        double throughput = totalOps > 0 ? totalOps / seconds : 0.0; // 每秒操作数

        // 5. 清理资源并获取统计数据
        kvssd.cleanup();
        KVSSD6.Stats stats = kvssd.getStats(); // 获取 KVSSD5 内部统计
        if (stats == null) {
            stats = new KVSSD6.Stats(); // 空指针防护：初始化空统计实例
            System.err.println("Warning: Failed to get Stats from KVSSD5, using empty Stats.");
        }

        // 6. 打印完整结果（基础指标 + 详细统计）
        System.out.println("\n==================================================");
        System.out.println("                 Workload Execution Result        ");
        System.out.println("==================================================");
        // 基础操作统计
        System.out.printf("Total ops: %,d (Read=%d, Write=%d)%n", totalOps, readOps, writeOps);
        System.out.printf("Elapsed time: %.2f s%n", seconds);
        System.out.printf("Throughput: %.2f ops/s%n", throughput);
        System.out.println("--------------------------------------------------");
        // KVSSD5 详细统计（所有字段）
        System.out.println("                 Detailed Stats (from KVSSD5)     ");
        System.out.println("--------------------------------------------------");
        System.out.printf("Application Write Count: %,d%n", stats.writeCount);
        System.out.printf("Application Read Count: %,d%n", stats.readCount);
        System.out.printf("GC Count: %,d%n", stats.gcCount);
        System.out.printf("Compaction Count: %,d%n", stats.compactionCount);
        System.out.printf("Write Amplification: %.2f%n", stats.writeAmplification);
        System.out.printf("Total Flash Writes (Bytes): %,d%n", stats.totalFlashWrites);
        System.out.printf("Total Flash Reads (Times): %,d%n", stats.totalFlashReads);
        System.out.println("--------------------------------------------------");
        System.out.println("Read Flash Access Distribution:");
        System.out.printf("  0 times flash access: %,d%n", stats.read0Flash);
        System.out.printf("  1 time flash access: %,d%n", stats.read1Flash);
        System.out.printf("  2 times flash access: %,d%n", stats.read2Flash);
        System.out.printf("  3 times flash access: %,d%n", stats.read3Flash);
        System.out.printf("  4 times flash access: %,d%n", stats.read4Flash);
        System.out.printf("  5 time flash access: %,d%n", stats.read5Flash);
        System.out.printf("  6 times flash access: %,d%n", stats.read6Flash);
        System.out.printf("  7 times flash access: %,d%n", stats.read7Flash);
        System.out.printf("  8 times flash access: %,d%n", stats.read8Flash);
        System.out.printf("  8+ times flash access: %,d%n", stats.readMoreFlash);
        System.out.println("==================================================");
    }
}