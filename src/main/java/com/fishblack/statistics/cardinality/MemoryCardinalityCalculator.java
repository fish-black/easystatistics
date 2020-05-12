package com.fishblack.statistics.cardinality;


import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashSet;

import static com.fishblack.statistics.Calculator.getLocalDateMillis;
import static com.fishblack.statistics.Calculator.getLocalDateTimeMillis;


public class MemoryCardinalityCalculator implements CardinalityCalculator {

    private static final long MAX_MEMORY_USAGE_LIMIT = 100 * 1024 * 1024;

    private HashSet<Object> distinctSet = new HashSet<>();
    private String dataType ;
    private long currentMemoryUsage = 0;

    public MemoryCardinalityCalculator(String datatype){
        dataType = datatype;
    }

    @Override
    public void add(Object data) {
        if (isCanProceed()) {
            String dataStr = String.valueOf(data);
            if (dataType.equals("string")) {
                distinctSet.add(dataStr);
                currentMemoryUsage += (40 + 2 * dataStr.length());
            } else if (dataType.equals("timestamp")) {
                distinctSet.add(getLocalDateTimeMillis((LocalDateTime) data));
                currentMemoryUsage += 24;
            } else if (dataType.equals("date")) {
                distinctSet.add(getLocalDateMillis((LocalDate) data));
                currentMemoryUsage += 24;
            } else if (dataType.equals("time")) {
                distinctSet.add(getLocalDateTimeMillis((LocalDateTime) data));
                currentMemoryUsage += 24;
            }else if (dataType.equals("decimal")) {
                distinctSet.add(dataStr);
                currentMemoryUsage += (40 + 2 * dataStr.length());
            } else if (dataType.equals("integer")) {
                distinctSet.add(data);
                currentMemoryUsage += 16;
            } else if (dataType.equals("double")) {
                distinctSet.add(data);
                currentMemoryUsage += 24;
            }
        }
    }

    @Override
    public long count() {
        if (isCanProceed()){
            return distinctSet.size();
        }
        else return -1;
    }

    private boolean isCanProceed(){
        return currentMemoryUsage < MAX_MEMORY_USAGE_LIMIT;
    }
}
