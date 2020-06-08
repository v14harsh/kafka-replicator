package com.example.demo.mirrortool;

import jdk.nashorn.internal.runtime.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class KafkaSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(KafkaSourceConnector.class);
    private KafkaSourceConnectorConfig connectorConfig;

    private PartitionMonitor partitionMonitor;

    @Override
    public String version() {
        return Version.version();
    }

    @Override
    public void start(Map<String, String> config) throws ConfigException {
        logger.info("Connector starting");
        connectorConfig = new KafkaSourceConnectorConfig(config);
        logger.info("Starting Partition Monitor to monitor source kafka cluster partitions");
        partitionMonitor = new PartitionMonitor(context, connectorConfig);
        partitionMonitor.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return KafkaSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<String> leaderTopicPartitions = partitionMonitor.getCurrentLeaderTopicPartitions().stream()
                .map(LeaderTopicPartition::toString).sorted() // Potential task performance/overhead improvement by roughly
                // grouping tasks and leaders
                .collect(Collectors.toList());
        int taskCount = Math.min(maxTasks, leaderTopicPartitions.size());
        if (taskCount < 1) {
            logger.warn("No tasks to start.");
            return new ArrayList<>();
        }
        return ConnectorUtils.groupPartitions(leaderTopicPartitions, taskCount).stream().map(leaderTopicPartitionsGroup -> {
            Map<String, String> taskConfig = new HashMap<>();
            taskConfig.putAll(connectorConfig.allAsStrings());
            taskConfig.put(KafkaSourceConnectorConfig.TASK_LEADER_TOPIC_PARTITION_CONFIG,
                    String.join(",", leaderTopicPartitionsGroup));
            return taskConfig;
        }).collect(Collectors.toList());
    }

    @Override
    public void stop() {
        logger.info("Connector received stop(). Cleaning Up.");
        partitionMonitor.shutdown();
        logger.info("Connector stopped.");
    }

    @Override
    public ConfigDef config() {
        return KafkaSourceConnectorConfig.CONFIG;
    }

}