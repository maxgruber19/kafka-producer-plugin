package io.jenkins.plugins.kafka;

import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;

public class KafkaProducerConfigParameter {
    private String key;
    private Object value;

    @DataBoundConstructor
    public KafkaProducerConfigParameter(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @DataBoundSetter
    public void setKey(String key) {
        this.key = key;
    }

    @DataBoundSetter
    public void setValue(Object value) {
        this.value = value;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "KafkaProducerConfigParameter [key=" + key + ", value=" + value + "]";
    }
}
