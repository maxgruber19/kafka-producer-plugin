package io.jenkins.plugins.kafka;

import hudson.model.FreeStyleBuild;
import hudson.model.FreeStyleProject;
import hudson.model.Label;
import org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition;
import org.jenkinsci.plugins.workflow.job.WorkflowJob;
import org.jenkinsci.plugins.workflow.job.WorkflowRun;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.jupiter.api.BeforeAll;
import org.jvnet.hudson.test.JenkinsRule;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Testcontainers
public class KafkaProducerBuilderTest {

    private static KafkaContainer kafka;

    @Rule
    public JenkinsRule jenkins = new JenkinsRule();

    final String topic = "my-test-topic";
    final String message = "Build Successful";
    final List<KafkaProducerConfigParameter> kafkaProducerConfigParameterList = new ArrayList<>();

    @BeforeClass
    public static void startKafka() throws InterruptedException {
        kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:6.2.1"))
                .withEnv("KAFKA_CREATE_TOPICS", "my-test-topic")
                .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(3)))
                .waitingFor(Wait.forLogMessage(".*\\[KafkaServer id=.*\\] started.*", 1));
        kafka.start();
        Thread.sleep(10000);
    }

    @Test
    public void testConfigRoundtrip() throws Exception {
        FreeStyleProject project = jenkins.createFreeStyleProject();
        project.getBuildersList().add(new KafkaProducerBuilder(kafka.getBootstrapServers(), topic, kafkaProducerConfigParameterList , message));
        project = jenkins.configRoundtrip(project);
        jenkins.assertEqualDataBoundBeans(
                new KafkaProducerBuilder(kafka.getBootstrapServers(), topic, kafkaProducerConfigParameterList , message), project.getBuildersList().get(0)
        );
    }

    @Test
    public void testBuild() throws Exception {
        FreeStyleProject project = jenkins.createFreeStyleProject();
        KafkaProducerBuilder builder = new KafkaProducerBuilder(kafka.getBootstrapServers(), topic, kafkaProducerConfigParameterList , message);
        project.getBuildersList().add(builder);

        FreeStyleBuild build = jenkins.buildAndAssertSuccess(project);
        jenkins.assertLogContains("Producing message to " + topic, build);
    }

    @Test
    public void testScriptedPipeline() throws Exception {
        String agentLabel = "my-agent";
        jenkins.createOnlineSlave(Label.get(agentLabel));
        WorkflowJob job = jenkins.createProject(WorkflowJob.class, "test-scripted-pipeline");
        String pipelineScript = "node { produce }";
        job.setDefinition(new CpsFlowDefinition(pipelineScript, true));
        WorkflowRun completedBuild = jenkins.assertBuildStatusSuccess(job.scheduleBuild2(0));
        String expectedString = "Producing message to " + topic;
        jenkins.assertLogContains(expectedString, completedBuild);
    }

}
