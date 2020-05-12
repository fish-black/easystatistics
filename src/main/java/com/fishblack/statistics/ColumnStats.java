package com.fishblack.statistics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.logging.Level;
import java.util.logging.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_DEFAULT)
public class ColumnStats {
    private static final Logger logger = Logger.getLogger(ColumnStats.class.getName());

    private String fieldName;
    private String dataType;
    private long cardinality;
    private long nullValueCount;
    private long avgColumnLen;
    private Object min;
    private Object max;
    private Object mean;
    private Object sum;

    @JsonProperty("name")
    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    @JsonProperty("type")
    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    @JsonProperty("nulls")
    public long getNullValueCount() {
        return nullValueCount;
    }

    public void setNullValueCount(long nullValueCount) {
        this.nullValueCount = nullValueCount;
    }

    @JsonProperty("distinct")
    public long getCardinality() {
        return cardinality;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }
    
    @JsonProperty("min")
    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    @JsonProperty("max")
    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }

    @JsonProperty("sum")
    public Object getSum() {
        return sum;
    }

    public void setSum(Object sum) {
        this.sum = sum;
    }

    @JsonProperty("mean")
    public Object getMean() {
        return mean;
    }

    public void setMean(Object mean) {
        this.mean = mean;
    }

    @JsonProperty("avg_len")
    public long getAvgColumnLen() {
        return avgColumnLen;
    }

    public void setAvgColumnLen(long avgColumnLen) {
        this.avgColumnLen = avgColumnLen;
    }

    /**
     * Generate Json string of this result.
     * @return Json string of this result.
     */
    public String toJSON() {
        try {
            return (new ObjectMapper()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.log(Level.WARNING, "Cannot convert ColumnStats to JSON", e);
            return "[Cannot convert ColumnStats to JSON]";
        }
    }
}
