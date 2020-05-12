package com.fishblack.statistics.cardinality;

import java.io.IOException;

public interface CardinalityCalculator {
    void add(Object data) throws IOException;
    long count() throws IOException;
}
