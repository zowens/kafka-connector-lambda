package io.github.zowens.kafkaconnectors.lambda;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Record {
    private Object key;
    private Object value;
    private long offset;

    @JsonProperty("Key")
    public Object getKey() {
        return key;
    }

    @JsonProperty("Key")
    public void setKey(Object key) {
        this.key = key;
    }

    @JsonProperty("Value")
    public Object getValue() {
        return value;
    }

    @JsonProperty("Value")
    public void setValue(Object value) {
        this.value = value;
    }

    @JsonProperty("Offset")
    public long getOffset() {
        return offset;
    }

    @JsonProperty("Offset")
    public void setOffset(long offset) {
        this.offset = offset;
    }
}
