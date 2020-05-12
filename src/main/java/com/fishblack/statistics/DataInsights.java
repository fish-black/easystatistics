package com.fishblack.statistics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataInsights {
    private static final Logger logger = Logger.getLogger(DataInsights.class.getName());

    private long rowCount;
    private int columnCount;
    private List<ColumnStats> columnStats = new ArrayList<>();

    @JsonProperty("rows")
    public long getRowCount() {
        return rowCount;
    }

    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    @JsonProperty("columns")
    public int getColumnCount() {
        return columnCount;
    }

    public void setColumnCount(int columnCount) {
        this.columnCount = columnCount;
    }

    @JsonProperty("stats")
    public List<ColumnStats> getColumnStats() {
        return columnStats;
    }

    public void setColumnStats(List<ColumnStats> columnStats) {
        this.columnStats = columnStats;
    }

    /**
     * Generate Json string of this result.
     * @return Json string of this result.
     */
    public String toJSON() {
        try {
            return (new ObjectMapper()).writeValueAsString(this);
        } catch (JsonProcessingException e) {
            logger.log(Level.WARNING, "Cannot convert ConvertDataInsights to JSON", e);
            return "[Cannot convert ConvertDataInsights to JSON]";
        }
    }
}
