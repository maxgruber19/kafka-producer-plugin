package io.jenkins.plugins.kafka;

import hudson.EnvVars;
import hudson.Extension;
import hudson.FilePath;
import hudson.Launcher;
import hudson.model.AbstractProject;
import hudson.model.Run;
import hudson.model.TaskListener;
import hudson.tasks.BuildStepDescriptor;
import hudson.tasks.Builder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import jenkins.tasks.SimpleBuildStep;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jenkinsci.Symbol;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;

public class KafkaProducerBuilder extends Builder implements SimpleBuildStep {

    private String bootstrapServers;
    private String topic;
    private List<KafkaProducerConfigParameter> producerConfigParameters;
    private Map<String, Object> producerConfig;
    private KafkaProducer<String, String> producer;

    @DataBoundConstructor
    public KafkaProducerBuilder(String bootstrapServers, String topic, List<KafkaProducerConfigParameter> producerConfigParameters) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.producerConfigParameters = producerConfigParameters;
        this.producerConfig = producerConfigParameters.stream().collect(Collectors.toMap(KafkaProducerConfigParameter::getKey, KafkaProducerConfigParameter::getValue));
        this.producerConfig.putIfAbsent("key.serializer", StringSerializer.class.getName());
        this.producerConfig.putIfAbsent("value.serializer", StringSerializer.class.getName());
        Thread.currentThread().setContextClassLoader(null);
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
    public void setProducerConfigParameters(List<KafkaProducerConfigParameter> producerConfigParameters) {
        this.producerConfigParameters = producerConfigParameters;
    }

    public List<KafkaProducerConfigParameter> getProducerConfigParameters() {
        return producerConfigParameters;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getTopic() {
        return topic;
    }

    @Override
    public void perform(Run<?, ?> run, FilePath workspace, EnvVars env, Launcher launcher, TaskListener listener)
            throws InterruptedException, IOException {
        listener.getLogger().println("Producing message to " + bootstrapServers);
        this.producer = new KafkaProducer<>(this.producerConfig);
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

    }
}
