package com.fishblack.statistics;

import com.fishblack.fastparquet.common.FieldMetadata;
import com.fishblack.fastparquet.common.SchemaConverter;
import com.fishblack.fastparquet.common.TempFile;
import com.fishblack.fastparquet.reader.ParquetColumnReader;
import com.fishblack.fastparquet.reader.ParquetColumnReaderImpl;
import com.fishblack.fastparquet.utils.ParquetAvroUtils;
import com.fishblack.statistics.cardinality.CardinalityCalculator;
import com.fishblack.statistics.cardinality.MemoryCardinalityCalculator;
import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.parquet.schema.Type;
import org.w3c.dom.Document;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Calculator {
    private static final Logger logger = Logger.getLogger(Calculator.class.getName());
    public static final String localDateTimeFormat = "yyyy-MM-dd HH:mm:ss.SSS";
    public static final String localDateFormat = "yyyy-MM-dd";
    public static final String timeFormat = "HH:mm:ss.SSS";
    public static final DateTimeFormatter localDateTimeFormatter = DateTimeFormatter.ofPattern(localDateTimeFormat);
    public static final DateTimeFormatter localDateFormatter = DateTimeFormatter.ofPattern(localDateFormat);
    public static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
    
    /**
     * Calculate the statistics from given parquet file for the dataset with typeOption.
     * @param tempParquetFile
     * @param typeoptionsXML
     * @return
     * @throws IOException
     */
    public DataInsights getStatisticsFromParquetFile(TempFile tempParquetFile, Document typeoptionsXML) throws IOException {
        List<FieldMetadata> fields = FieldMetadata.getPublishedFields(typeoptionsXML);
        return getDataInsightsFromParquetFile(tempParquetFile, fields);
    }

    public DataInsights getDataInsightsFromParquetFile(TempFile tempParquetFile, List<FieldMetadata> fields) throws IOException {
        Schema sc = SchemaConverter.toAvroSchema(fields);
        List<Schema.Field> schemaList = sc.getFields();
        DataInsights dataInsights = new DataInsights();
        HashMap<String, String> fieldNameMap = new HashMap<>();
        HashMap<String, String> fieldTypeMap = new HashMap<>();
        for (int i=0; i<fields.size(); i++){
            String originalColumnName = StringEscapeUtils.escapeJava(fields.get(i).getFieldName());
            String fieldName = schemaList.get(i).name();
            String fieldType = convertOACDataType(fields.get(i).getFieldType());
            fieldNameMap.put(fieldName, originalColumnName);
            fieldTypeMap.put(fieldName, fieldType);
        }
        for(Type type : ParquetAvroUtils.getParquetFileColumns(tempParquetFile.getPath())) {
            String fieldName = type.getName();
            String originName = fieldNameMap.get(fieldName);
            String fieldType = fieldTypeMap.get(fieldName);
            dataInsights.setColumnCount(dataInsights.getColumnCount() + 1);
            calColumnStats(dataInsights, originName, fieldName, fieldType, tempParquetFile);
        }
        return dataInsights;
    }

    private void calColumnStats(DataInsights dataInsights, String originalFieldName, String fieldName, String fieldType,
                                TempFile tempParquetFile) throws IOException{
        try (ParquetColumnReader reader = new ParquetColumnReaderImpl(tempParquetFile.getPath(), fieldName)) {
            ColumnStats columnStats = new ColumnStats();
            columnStats.setFieldName(originalFieldName);
            columnStats.setDataType(fieldType);
            CardinalityCalculator cardinalityCalculator = new MemoryCardinalityCalculator(fieldType);
            long currentRowNum = 0;
            long totalLength = 0;
            while (reader.hasNext()) {
                Object data = reader.next();
                currentRowNum ++;
                dataInsights.setRowCount(currentRowNum);
                if (data != null) {
                    cardinalityCalculator.add(data);
                    totalLength += String.valueOf(data).length();
                }
                recordDataStats(columnStats, data, currentRowNum, totalLength);
            }
            columnStats.setCardinality(cardinalityCalculator.count());
            if (fieldType.equals("timestamp")){
                LocalDateTime minTimestamp = (LocalDateTime) columnStats.getMin();
                LocalDateTime maxTimestamp = (LocalDateTime) columnStats.getMax();
                columnStats.setMin(minTimestamp.format(localDateTimeFormatter));
                columnStats.setMax(maxTimestamp.format(localDateTimeFormatter));
            }
            else if (fieldType.equals("date")){
                LocalDate minDate = (LocalDate) columnStats.getMin();
                LocalDate maxDate = (LocalDate) columnStats.getMax();
                columnStats.setMin(minDate.format(localDateFormatter));
                columnStats.setMax(maxDate.format(localDateFormatter));
            }
            else if (fieldType.equals("time")){
                LocalDateTime minTime = (LocalDateTime) columnStats.getMin();
                LocalDateTime maxTime = (LocalDateTime) columnStats.getMax();
                columnStats.setMin(minTime.format(timeFormatter));
                columnStats.setMax(maxTime.format(timeFormatter));
            }
            dataInsights.getColumnStats().add(columnStats);
        }
        catch (IllegalArgumentException ex){
            logger.log(Level.WARNING, "Parquet column reader read failed on column:"+fieldName, ex);
        }

    }

    private String convertOACDataType(String oacType){
        String type = "";
        String lowerCaseType = oacType.toLowerCase();
        if (lowerCaseType.startsWith("varchar")){
            type = "string";
        }
        else if (lowerCaseType.equals("double")){
            type = "double";
        }
        else if (lowerCaseType.startsWith("number")){
            type = "decimal";
        }
        else if (lowerCaseType.equals("integer")){
            type = "integer";
        }
        else if (lowerCaseType.equals("date")){
            type = "date";
        }
        else if (lowerCaseType.equals("time")){
            type = "time";
        }
        else if (lowerCaseType.equals("timestamp")){
            type = "timestamp";
        }
        return type;
    }

    private void recordDataStats(ColumnStats columnStats, Object data, long currentRowNumber, long totalLength) {
        if (data == null) {
            addNullValueCount(columnStats);
            return;
        }

        String dataStr = String.valueOf(data);
        if (!columnStats.getDataType().equals("string") && StringUtils.isEmpty(dataStr)) {
            addNullValueCount(columnStats);
            return;
        }

        switch (columnStats.getDataType()) {
            case "double":
                columnStats.setSum(columnStats.getSum() == null ? (Double)data : (Double) columnStats.getSum() + (Double)data);
                columnStats.setMean((double) columnStats.getSum() / currentRowNumber);
                columnStats.setMin(Math.min(columnStats.getMin() == null ? (Double)data: (Double) columnStats.getMin(), (Double)data));
                columnStats.setMax(Math.max(columnStats.getMax() == null ? (Double)data : (Double) columnStats.getMax(), (Double)data));
                break;
            case "integer":
                columnStats.setSum(columnStats.getSum() == null ? ((Integer)data).longValue() : (Long) columnStats.getSum() + ((Integer)data).longValue());
                columnStats.setMean(((Long)columnStats.getSum()).doubleValue() / currentRowNumber );
                columnStats.setMin(Math.min(columnStats.getMin() == null ? Integer.parseInt(dataStr): Integer.valueOf(columnStats.getMin().toString()), Integer.parseInt(dataStr)));
                columnStats.setMax(Math.max(columnStats.getMax() == null ? Integer.parseInt(dataStr) : Integer.valueOf(columnStats.getMax().toString()), Integer.parseInt(dataStr)));
                break;
            case "decimal":
                BigDecimal value = (BigDecimal)data;
                columnStats.setSum(columnStats.getSum() == null ? value : ((BigDecimal)columnStats.getSum()).add(value));
                columnStats.setMean(((BigDecimal) columnStats.getSum()).divide(new BigDecimal(currentRowNumber), RoundingMode.UP ));
                BigDecimal orgMin = columnStats.getMin() == null ? value: (BigDecimal)columnStats.getMin();
                BigDecimal orgMax = columnStats.getMax() == null ? value: (BigDecimal)columnStats.getMax();
                columnStats.setMin(value.min(orgMin));
                columnStats.setMax(value.max(orgMax));
                break;
            case "timestamp":
                LocalDateTime timestamp = (LocalDateTime) data;
                if (columnStats.getMin() == null || ((LocalDateTime)columnStats.getMin()).isAfter(timestamp)) {
                    columnStats.setMin(timestamp);
                }
                if (columnStats.getMax() == null || ((LocalDateTime)columnStats.getMax()).isBefore(timestamp)) {
                    columnStats.setMax(timestamp);
                }
                break;
            case "date":
                LocalDate date = (LocalDate) data;
                if (columnStats.getMin() == null || ((LocalDate)columnStats.getMin()).isAfter(date)) {
                    columnStats.setMin(date);
                }
                if (columnStats.getMax() == null || ((LocalDate)columnStats.getMax()).isBefore(date)) {
                    columnStats.setMax(date);
                }
                break;
            case "time":
                LocalDateTime time = (LocalDateTime) data;
                if (columnStats.getMin() == null || ((LocalDateTime)columnStats.getMin()).isAfter(time)) {
                    columnStats.setMin(time);
                }
                if (columnStats.getMax() == null || ((LocalDateTime)columnStats.getMax()).isBefore(time)) {
                    columnStats.setMax(time);
                }
                break;
            case "string":
                columnStats.setAvgColumnLen( totalLength / currentRowNumber );
                break;
            default:
                break;
        }
    }

    private void addNullValueCount(ColumnStats columnStats){
        long count = columnStats.getNullValueCount();
        columnStats.setNullValueCount(count + 1);
    }

    public static long getLocalDateTimeMillis(LocalDateTime localDateTime){
        return localDateTime.toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
    }

    public static long getLocalDateMillis(LocalDate localDate){
        return localDate.atStartOfDay().toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
    }
}
