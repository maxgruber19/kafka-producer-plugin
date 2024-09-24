package io.jenkins.plugins.kafka;

import hudson.model.FreeStyleProject;
import lombok.extern.java.Log;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.jvnet.hudson.test.JenkinsRule;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Log
@Testcontainers
public class KafkaProducerBuilderTest {

    private static Admin kafkaAdmin = null;
    private static Properties properties = new Properties();
    private static final String topic = "jenkins.builds.states";
    private static final String message = "build_successful";

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
            .withStartupTimeout(Duration.ofMinutes(5))
            .waitingFor(Wait.forHealthcheck())
            .waitingFor(Wait.forListeningPort())
            .waitingFor(Wait.forLogMessage(".*started \\(kafka.server.KafkaServer\\).*", 1));

    @Rule
    public JenkinsRule jenkins = new JenkinsRule();


    @BeforeClass
    public static void startKafka() throws InterruptedException, ExecutionException {
        kafka.start();
        properties.put("bootstrap.servers", kafka.getBootstrapServers());

        log.info("creating topic " + topic);
        kafkaAdmin =  Admin.create(properties);
        CreateTopicsResult result = kafkaAdmin.createTopics(java.util.List.of(new NewTopic("jenkins.builds.states", 1 , (short) 1)));
        log.info("topics found in cluster: " + kafkaAdmin.listTopics().names().get());

    }

    @Test
    public void testConfigRoundtrip() throws Exception {

        List<KafkaProducerConfigParameter> kafkaProducerConfigParameterList = new ArrayList<>();
        kafkaProducerConfigParameterList.add(new KafkaProducerConfigParameter("test.property.1", "active"));
        kafkaProducerConfigParameterList.add(new KafkaProducerConfigParameter("test.property.2", "inactive"));

        KafkaProducerBuilder kafkaProducerBuilder = new KafkaProducerBuilder(kafka.getBootstrapServers(), topic, kafkaProducerConfigParameterList);

        FreeStyleProject project = jenkins.createFreeStyleProject();
        project.getBuildersList().add(kafkaProducerBuilder);
        project = jenkins.configRoundtrip(project);
        jenkins.assertEqualDataBoundBeans(
                kafkaProducerBuilder, project.getBuildersList().get(0)
        );
    }

}
