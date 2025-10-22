package com.ssd;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Random;

/**
 * KVGenerator（修改版）：Key/Value 均为字符串格式，移除 bytesToHex 编码
 * 输出 CSV 格式： op,key_str,value_str
 *  - op = R 或 W
 *  - 对于 R 行，value 字段为空
 */
public class KVGeneratorString {

    public static void main(String[] args) throws Exception {
        // 默认配置（与原逻辑一致）
        String output = "kv_workload.csv";
        long ops = 100000;
        double readRatio = 0.8;
        int numKeys = 100000;
        String keySizeMode = "fixed";
        int keySize = 20; // Key 的字符串长度（字节数，UTF-8 编码下1个字符=1字节）
        int keySizeMin = 10, keySizeMax = 40;
        String valSizeMode = "fixed";
        int valSize = 1000; // Value 的字符串长度
        int valSizeMin = 100, valSizeMax = 2000;
        String keyDist = "uniform";
        double zipfS = 0.99;
        long seed = System.currentTimeMillis();

        // 解析参数（与原逻辑一致，无需修改）
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--output": output = args[++i]; break;
                case "--ops": ops = Long.parseLong(args[++i]); break;
                case "--readRatio": readRatio = Double.parseDouble(args[++i]); break;
                case "--numKeys": numKeys = Integer.parseInt(args[++i]); break;
                case "--keySizeMode": keySizeMode = args[++i]; break;
                case "--keySize": keySize = Integer.parseInt(args[++i]); break;
                case "--keySizeMin": keySizeMin = Integer.parseInt(args[++i]); break;
                case "--keySizeMax": keySizeMax = Integer.parseInt(args[++i]); break;
                case "--valSizeMode": valSizeMode = args[++i]; break;
                case "--valSize": valSize = Integer.parseInt(args[++i]); break;
                case "--valSizeMin": valSizeMin = Integer.parseInt(args[++i]); break;
                case "--valSizeMax": valSizeMax = Integer.parseInt(args[++i]); break;
                case "--keyDist": keyDist = args[++i]; break;
                case "--zipfS": zipfS = Double.parseDouble(args[++i]); break;
                case "--seed": seed = Long.parseLong(args[++i]); break;
                default:
                    System.err.println("Unknown arg: " + args[i]);
                    System.exit(1);
            }
        }

        // 参数校验（与原逻辑一致，无需修改）
        if (keySizeMode.equals("range") && keySizeMin > keySizeMax) {
            throw new IllegalArgumentException("keySizeMin > keySizeMax");
        }
        if (valSizeMode.equals("range") && valSizeMin > valSizeMax) {
            throw new IllegalArgumentException("valSizeMin > valSizeMax");
        }
        if (!(keyDist.equals("uniform") || keyDist.equals("zipf"))) {
            throw new IllegalArgumentException("keyDist must be uniform or zipf");
        }

        // 打印配置信息（更新表头说明）
        System.out.printf("Generating %d ops -> %s (readRatio=%.2f, numKeys=%d)\n",
                ops, output, readRatio, numKeys);
        System.out.printf("Key size mode=%s, val size mode=%s, keyDist=%s, seed=%d\n",
                keySizeMode, valSizeMode, keyDist, seed);
        System.out.println("Note: Key/Value are plain string (no hex/base64 encoding)");

        Random rnd = new Random(seed);
        SecureRandom sec = new SecureRandom(); // 用于生成随机字符串的字符选择

        ZipfGenerator zipf = null;
        if (keyDist.equals("zipf")) {
            zipf = new ZipfGenerator(numKeys, zipfS, rnd);
        }

        try (BufferedWriter w = new BufferedWriter(new FileWriter(output))) {
            // 1. 修改：CSV 表头从 "key_hex,value_base64" 改为 "key_str,value_str"
            w.write("op,key_str,value_str\n");

            for (long i = 0; i < ops; i++) {
                boolean isRead = rnd.nextDouble() < readRatio;
                int keyId;
                // 选择 Key ID（与原逻辑一致：均匀/Zipf 分布）
                if (keyDist.equals("uniform")) {
                    keyId = rnd.nextInt(numKeys); // [0, numKeys)
                } else {
                    keyId = zipf.next() - 1; // Zipf 生成 1..N，转成 0..N-1
                }

                // 确定当前 Key/Value 的字符串长度（与原逻辑一致）
                int curKeySize = (int) determineSize(keySizeMode, keySize, keySizeMin, keySizeMax, rnd);
                int curValSize = (int) determineSize(valSizeMode, valSize, valSizeMin, valSizeMax, rnd);

                // 2. 修改：直接生成字符串格式的 Key（移除 bytes 转 hex 步骤）
                String keyStr = keyIdToKeyString(keyId, curKeySize, rnd);

                if (isRead) {
                    // 读操作：Value 字段为空，直接写字符串 Key
                    w.write("R," + keyStr + ",\n");
                } else {
                    // 3. 修改：直接生成字符串格式的 Value（移除 bytes 转 base64 步骤）
                    String valStr = generateRandomString(curValSize, sec);
                    // 写操作：Key 和 Value 均为字符串
                    w.write("W," + keyStr + "," + valStr + "\n");
                }

                // 定期刷新缓冲区（与原逻辑一致）
                if ((i & 0xFFFFF) == 0 && i > 0) {
                    w.flush();
                }
            }
        }

        System.out.println("Done. Output: " + output);
    }

    // 原逻辑：确定 Key/Value 大小（无需修改）
    private static long determineSize(String mode, int fixed, int min, int max, Random rnd) {
        switch (mode) {
            case "fixed": return fixed;
            case "range": return min + rnd.nextInt(max - min + 1);
            default: throw new IllegalArgumentException("unknown size mode: " + mode);
        }
    }

    /**
     * 新增：将 Key ID 转为指定长度的字符串 Key
     * 逻辑：以 Key ID 为基础，不足长度补随机字母/数字，超过长度截断
     * @param keyId 唯一 Key ID
     * @param wantSize 目标字符串长度
     * @param rnd 随机数生成器（保证同 ID+同长度生成同 Key）
     * @return 固定长度的字符串 Key
     */
    private static String keyIdToKeyString(int keyId, int wantSize, Random rnd) {
        // 基础字符串：Key ID + 固定前缀（确保唯一性）
        String base = "key_" + keyId; // 格式如 "key_1234"
        StringBuilder sb = new StringBuilder(wantSize);

        // 1. 先添加基础字符串（保证同 ID 基础一致）
        for (int i = 0; i < base.length() && i < wantSize; i++) {
            sb.append(base.charAt(i));
        }

        // 2. 若长度不足，补充随机字母/数字（保证长度达标，且同种子生成同内容）
        if (sb.length() < wantSize) {
            // 可选择的字符集：字母（大小写）+ 数字（共62个字符）
            String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
            // 固定随机种子偏移（确保同 keyId + 同 wantSize 生成同补充内容）
            long fixedSeed = keyId + (long) wantSize * 1000000; // 避免不同 ID/长度种子冲突
            Random fixedRnd = new Random(fixedSeed);

            while (sb.length() < wantSize) {
                int charIndex = fixedRnd.nextInt(chars.length());
                sb.append(chars.charAt(charIndex));
            }
        }

        // 3. 若长度超过（理论上不会，base 较短），直接截断
        return sb.substring(0, wantSize);
    }

    /**
     * 新增：生成指定长度的随机字符串（用于 Value）
     * @param length 目标字符串长度
     * @param sec 安全随机数生成器（保证随机性）
     * @return 随机字符串
     */
    private static String generateRandomString(int length, SecureRandom sec) {
        if (length <= 0) return "";
        // 字符集：字母（大小写）+ 数字 + 常见符号（共93个字符，避免CSV冲突的逗号/引号）
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_-+=[]{}|;:.<>?";
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            // 随机选择字符集中的字符
            int charIndex = sec.nextInt(chars.length());
            sb.append(chars.charAt(charIndex));
        }

        return sb.toString();
    }

    // 移除：原 bytesToHex 方法（不再需要）

    /**
     * 原 Zipf 生成器（无需修改）：生成 1..N 的 Key ID 分布
     */
    static class ZipfGenerator {
        private final int size;
        private final double skew;
        private final double[] cdf; // 累积分布函数
        private final Random rnd;

        ZipfGenerator(int size, double skew, Random rnd) {
            if (size < 1) throw new IllegalArgumentException("size>=1");
            this.size = size;
            this.skew = skew;
            this.rnd = rnd;
            this.cdf = new double[size];
            // 计算归一化常数和累积分布
            double sum = 0.0;
            for (int i = 1; i <= size; i++) {
                sum += 1.0 / Math.pow(i, skew);
            }
            double acc = 0.0;
            for (int i = 1; i <= size; i++) {
                acc += (1.0 / Math.pow(i, skew)) / sum;
                cdf[i-1] = acc;
            }
            cdf[size-1] = 1.0; // 确保最后一位是 1.0
        }

        int next() {
            double u = rnd.nextDouble();
            // 二分查找找到对应的 Key ID
            int lo = 0, hi = size - 1;
            while (lo < hi) {
                int mid = (lo + hi) >>> 1;
                if (u <= cdf[mid]) hi = mid;
                else lo = mid + 1;
            }
            return lo + 1;
        }
    }
}