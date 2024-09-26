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
import jenkins.tasks.SimpleBuildStep;
import lombok.Getter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.jenkinsci.Symbol;
import org.kohsuke.stapler.DataBoundConstructor;
import org.kohsuke.stapler.DataBoundSetter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Getter
public class KafkaProducerBuilder extends Builder implements SimpleBuildStep {

    private String bootstrapServers;
    private String topic;
    private String producerConfigParameters;
    private Object message;

    @DataBoundConstructor
    public KafkaProducerBuilder(String bootstrapServers, String topic, String producerConfigParameters, String message) {
        this.topic = topic;
        this.bootstrapServers = bootstrapServers;
        this.message = message;
        this.producerConfigParameters = producerConfigParameters;
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
    public void setProducerConfigParameters(String producerConfigParameters) {
        this.producerConfigParameters = producerConfigParameters;
    }

    @Override
    public void perform(Run<?, ?> run, FilePath workspace, EnvVars env, Launcher launcher, TaskListener listener) throws InterruptedException, IOException {

        listener.getLogger().println("Producing message to " + bootstrapServers);

        Map<String, Object> producerConfig = new HashMap<>();

        ClassLoader original = Thread.currentThread().getContextClassLoader();
        Thread.currentThread().setContextClassLoader(null);

        try (KafkaProducer<String, Object> producer = new KafkaProducer<>(producerConfig)){
            ProducerRecord<String, Object> record = new ProducerRecord<>(topic, message);
            producer.send(record).get();
            Thread.currentThread().setContextClassLoader(original);
        } catch (ExecutionException e) {
            System.out.println(e.getMessage());
            throw new RuntimeException(e);
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

    }
}
