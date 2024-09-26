package io.jenkins.plugins.kafka;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import hudson.EnvVars;
import hudson.Extension;
import hudson.FilePath;
import hudson.Launcher;
import hudson.model.AbstractProject;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.Builder;
import jenkins.tasks.SimpleBuildStep;
import lombok.Getter;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jenkinsci.Symbol;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;
import org.kohsuke.stapler.StaplerRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Getter
public class KafkaProducerBuilder extends Builder implements SimpleBuildStep {

    private String bootstrapServers;
    private String topic;
    private List<KafkaProducerConfigParameter> producerConfigParameters = new ArrayList<>();
    private Object message;

    @DataBoundConstructor
    public KafkaProducerBuilder(String bootstrapServers, String topic, List<KafkaProducerConfigParameter> producerConfigParameters, String message) {
        if (null != topic) {
            this.topic = topic;
        }
        if (null != bootstrapServers) {
            this.bootstrapServers = bootstrapServers;
        }
        if (null != producerConfigParameters) {
            this.producerConfigParameters = producerConfigParameters;
        }
        if (null != message) {
            this.message = message;
        }
    }

    @DataBoundSetter
    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    @DataBoundSetter
    public void setTopic(String topic) {
        this.topic = topic;
    }

    @DataBoundSetter
    public void setMessage(Object message) {
        this.message = message;
    }

    @DataBoundSetter
    public void setProducerConfigParameters(List<KafkaProducerConfigParameter> producerConfigParameters) {
        this.producerConfigParameters = producerConfigParameters;
    }

    @Override
    public void perform(Run<?, ?> run, FilePath workspace, EnvVars env, Launcher launcher, TaskListener listener) throws InterruptedException, IOException {

        // https://stackoverflow.com/questions/37363119/kafka-producer-org-apache-kafka-common-serialization-stringserializer-could-no
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);

        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.putIfAbsent("bootstrap.servers", bootstrapServers);
        producerConfig.putIfAbsent("key.serializer", StringSerializer.class.getName());
        producerConfig.putIfAbsent("value.serializer", StringSerializer.class.getName());
        producerConfigParameters.forEach(parameter ->
                producerConfig.putIfAbsent(parameter.getKey(), parameter.getValue())
        );

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(producerConfig)){
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, message);
            producer.send(record).get();

        } catch (ExecutionException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Symbol("produce")
    @Extension
    public static final class DescriptorImpl extends BuildStepDescriptor<Builder> {

        @Override
        public boolean isApplicable(Class<? extends AbstractProject> aClass) {
            return true;
        }

        @Override
        public String getDisplayName() {
            return "Produce messages to Apache Kafka";
        }

        @Override
        public Builder newInstance(@Nullable StaplerRequest req, @NonNull JSONObject formData) throws FormException {
            return super.newInstance(req, formData);
        }
    }

    @Getter
    public static class KafkaProducerConfigParameter {
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

        @Override
        public String toString() {
            return "KafkaProducerConfigParameter [key=" + key + ", value=" + value + "]";
        }

    }

}
