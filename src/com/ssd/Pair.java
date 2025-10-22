package com.ssd;
/**
 * 键值对工具类
 */
public class Pair<K, V> {
  public K first;
  public V second;

  public Pair(K first, V second) {
    this.first = first;
    this.second = second;
  }
}