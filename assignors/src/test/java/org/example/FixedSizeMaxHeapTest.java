package org.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

class FixedSizeMaxHeapTest {
    @Test void shouldAddElementsWithinMaxSize() {
        FixedSizeMaxHeap<Integer> heap = new FixedSizeMaxHeap<>(3);
        heap.add(10); heap.add(20); heap.add(30);

        assertEquals(3, heap.size());
        assertEquals(3, heap.entryCount());
    }

    @Test void shouldMaintainMaxSizeWhenAddingMoreElements() {
        FixedSizeMaxHeap<Integer> heap = new FixedSizeMaxHeap<>(3);

        heap.add(10); heap.add(20); heap.add(30); heap.add(40); heap.add(50);

        assertEquals(3, heap.size());
        assertEquals(5, heap.entryCount());
    }

    @Test void shouldKeepLargestElementsWithNaturalOrdering() {
        FixedSizeMaxHeap<Integer> heap = new FixedSizeMaxHeap<>(3);

        heap.add(5); heap.add(10); heap.add(3); heap.add(15); heap.add(8); heap.add(20);

        assertEquals(3, heap.size());

        Set<Integer> contents = new HashSet<>();
        heap.iterator().forEachRemaining(contents::add);
        assertEquals(Set.of(10, 15, 20), contents);
    }

    @Test void shouldHandleDuplicateElementsCorrectly() {
        FixedSizeMaxHeap<Integer> heap = new FixedSizeMaxHeap<>(3);

        heap.add(10); heap.add(10); heap.add(5); heap.add(15); heap.add(10);

        assertEquals(3, heap.size());
        assertEquals(5, heap.entryCount());

        List<Integer> contents = new ArrayList<>();
        heap.iterator().forEachRemaining(contents::add);
        Collections.sort(contents);
        assertEquals(List.of(10, 10, 15), contents);
    }
}
