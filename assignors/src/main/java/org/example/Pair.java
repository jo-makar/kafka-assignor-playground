package org.example;

import java.util.Map;

public class Pair<T,U> {
    private T first;
    private U second;

    public Pair(T first, U second) {
        this.first = first;
        this.second = second;
    }

    public Pair(Map.Entry<T,U> entry) {
        this.first = entry.getKey();
        this.second = entry.getValue();
    }

    public T getFirst() {
        return first;
    }

    public U getSecond() {
        return second;
    }
}
