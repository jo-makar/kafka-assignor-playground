package org.example;

import java.util.Iterator;
import java.util.PriorityQueue;

public class FixedSizeMaxHeap<T extends Comparable<T>> {
    // Minimum heap of the largest elements
    private final PriorityQueue<T> minHeap;

    private final int maxSize;
    private int entryCount = 0;

    public FixedSizeMaxHeap(int maxSize) {
        this.minHeap = new PriorityQueue<>();
        this.maxSize = maxSize;
    }

    public void add(T entry) {
        if (entryCount < maxSize) {
            minHeap.add(entry);
        } else {
            // If the entry is greater than the smallest element in the heap,
            // then discared the smallest element and add the entry
            if (minHeap.peek().compareTo(entry) == -1) {
                minHeap.poll();
                minHeap.add(entry);
            }
        }
        entryCount++;
    }

    public Iterator<T> iterator() {
        return minHeap.iterator();
    }

    public int size() {
        return minHeap.size();
    }

    public int entryCount() {
        return entryCount;
    }
}
