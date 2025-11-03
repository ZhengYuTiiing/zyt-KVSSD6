package com.ssd;




import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

/**
 * 按顺序写入Key，再按相同顺序读取（移除分布策略，固定读写顺序严格一致）
 */
public class GeneratorNew {
    public static void main(String[] args) throws Exception {
        // 默认配置
        String output = "kv_workload.csv";
        long totalOps = 100000;  // 总操作数
        double readRatio = 0.5;  // 读操作比例（0~1）
        int keySize = 20;        // Key固定长度
        int valSize = 1000;      // Value固定长度
        long seed = System.currentTimeMillis();

        // 解析参数（移除keyDist/zipfS等分布相关参数）
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--output":
                    output = args[++i];
                    break;
                case "--ops":
                    totalOps = Long.parseLong(args[++i]);
                    break;
                case "--readRatio":
                    readRatio = Double.parseDouble(args[++i]);
                    break;
                case "--keySize":
                    keySize = Integer.parseInt(args[++i]);
                    break;
                case "--valSize":
                    valSize = Integer.parseInt(args[++i]);
                    break;
                case "--seed":
                    seed = Long.parseLong(args[++i]);
                    break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
                    System.exit(1);
            }
        }

        // 参数校验
        if (totalOps <= 0) {
            throw new IllegalArgumentException("totalOps must be > 0");
        }
        if (readRatio < 0 || readRatio > 1) {
            throw new IllegalArgumentException("readRatio must be between 0 and 1");
        }
        if (keySize < 1) {
            throw new IllegalArgumentException("keySize must be > 0");
        }
        if (valSize < 0) {
            throw new IllegalArgumentException("valSize must be >= 0");
        }

        // 计算写/读操作数量
        long writeOps = (long) (totalOps * (1 - readRatio));
        long readOps = totalOps - writeOps;
        // 确保至少有1次写操作（否则读操作无Key可用）
        if (writeOps == 0) {
            writeOps = 1;
            readOps = totalOps - 1;
        }

        // 打印配置信息
        System.out.printf("Generating %d total ops (write: %d, read: %d) -> %s%n",
                totalOps, writeOps, readOps, output);
        System.out.printf("Key size: %d, Value size: %d, seed: %d%n",
                keySize, valSize, seed);
        System.out.println("Note: 按顺序写入Key，再按相同顺序读取");

        SecureRandom sec = new SecureRandom();
        sec.setSeed(seed);

        // 存储写入的Key顺序（用于按顺序读）
        List<String> writtenKeys = new ArrayList<>((int) writeOps);

        try (BufferedWriter w = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(output), StandardCharsets.UTF_8))) {
            w.write("op,key_str,value_str\n");

            // 第一阶段：按顺序生成写操作（Key格式：key_0000...，补齐至keySize）
            System.out.println("生成写操作...");
            for (long i = 0; i < writeOps; i++) {
                // 生成有序Key：key_ + 数字索引（如key_0, key_1...）
                String baseKey = "key_" + i;
                // 补齐至指定keySize（不足补随机字符，超过截断）
                String keyStr = padKeyToSize(baseKey, keySize, i);
                writtenKeys.add(keyStr);  // 记录写入顺序

                // 生成固定长度的Value
                String valStr = generateFixedLengthValue(valSize, sec);

                // 写入CSV
                w.write("W," + keyStr + "," + valStr + "\n");

                // 定期刷新缓冲区
                if ((i & 0xFFFFF) == 0 && i > 0) {
                    w.flush();
                }
            }

            // 第二阶段：按写入顺序生成读操作
            System.out.println("生成读操作...");
            for (long i = 0; i < readOps; i++) {
                // 按写入顺序循环读取（若读操作数 > 写操作数，重复读取）
                int keyIndex = (int) (i % writtenKeys.size());
                String keyStr = writtenKeys.get(keyIndex);
                w.write("R," + keyStr + ",\n");

                // 定期刷新缓冲区
                if ((i & 0xFFFFF) == 0 && i > 0) {
                    w.flush();
                }
            }
        }

        System.out.println("Done. Output: " + output);
        System.out.printf("共生成 %d 个唯一写Key，总操作数: %d%n", writtenKeys.size(), totalOps);
    }

    /**
     * 将Key补齐至指定长度（不足补随机字符，超过截断）
     * @param baseKey 基础Key（如key_0）
     * @param targetSize 目标长度
     * @param index Key索引（用于固定随机补充字符，确保同索引Key一致）
     * @return 固定长度的Key
     */
    private static String padKeyToSize(String baseKey, int targetSize, long index) {
        if (baseKey.length() >= targetSize) {
            return baseKey.substring(0, targetSize);  // 超过长度则截断
        }

        // 不足长度则补充随机字符（字母+数字）
        StringBuilder sb = new StringBuilder(baseKey);
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        SecureRandom rnd = new SecureRandom();
        rnd.setSeed(index);  // 基于索引固定随机种子，确保同索引Key补充字符一致

        while (sb.length() < targetSize) {
            int charIndex = rnd.nextInt(chars.length());
            sb.append(chars.charAt(charIndex));
        }
        return sb.toString();
    }

    /**
     * 生成固定长度的随机Value
     */
    private static String generateFixedLengthValue(int length, SecureRandom sec) {
        if (length <= 0) {
            return "";
        }
        // 字符集：避免CSV冲突的逗号和引号
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_-+=[]{}|;:.<>?";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            int charIndex = sec.nextInt(chars.length());
            sb.append(chars.charAt(charIndex));
        }
        return sb.toString();
    }
}