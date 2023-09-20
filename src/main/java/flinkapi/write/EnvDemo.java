package flinkapi.write;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author: YK.Leo
 * @Date: 2023-05-14 15:12
 * @Version: 1.0
 */

// Succeed at local ！！！
public class EnvDemo {
    private static final Logger LOG = LoggerFactory.getLogger(EnvDemo.class);

    public static void main(String[] args) throws Exception {

        LOG.debug("Stop Flink example job");

        //加载自定义参数
        String propertiesFilePath = "/Users/leicq/flink_spaces/quickstart/src/main/resources/flink-conf.properties";
        ParameterTool parameters = ParameterTool.fromPropertiesFile(propertiesFilePath);

        // 定义一个配置 import org.apache.flink.configuration.Configuration;包下
        Configuration configuration = new Configuration();
//        // 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8082);

        // flink 运行时指标
//        Properties props = new Properties();
////        props.put("metrics.reporter.jmx.factory.class", "org.apache.flink.metrics.jmx.JMXReporterFactory");
////        props.put(RestOptions.PORT, 8082);
//        Configuration conf = ConfigurationUtils.createConfiguration(props);
       StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        env.getConfig().setGlobalJobParameters(parameters);

        // checkpoint 缺省是禁用的，需要主动打开
        env.enableCheckpointing(1000);

        // 设置模式为精确一次 (这是默认值)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //CP保存目录
        env.getCheckpointConfig().setCheckpointStorage(parameters.getRequired("state.checkpoints.dir"));
// 确认 checkpoints 之间的时间会进行 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameters.getInt("execution.checkpointing.interval"));
// Checkpoint 必须在一分钟内完成，否则就会被抛弃
        env.getCheckpointConfig().setCheckpointTimeout(parameters.getInt("execution.checkpointing.timeout"));
// 允许两个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(parameters.getInt("execution.checkpointing.tolerable-failed-checkpoints"));

// 同一时间只允许一个 checkpoint 进行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(parameters.getShort("execution.checkpointing.max-concurrent-checkpoints"));

// 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// 开启实验性的 unaligned checkpoints, 当前不支持非对齐
       // env.getCheckpointConfig().enableUnalignedCheckpoints();

    // 执行环境使用当前配置
//      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        TableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 0. Create a Catalog and a Table
        tableEnv.executeSql("CREATE CATALOG my_catalog WITH (\n" +
                "    'type'='paimon',\n" +
                "    'warehouse'='file:///Users/leicq/share_dir/paimon'\n" +
//                "    'warehouse'='hdfs://10.5.106.212:8020/paimon'\n" +
                ")");

        tableEnv.executeSql("USE CATALOG my_catalog");

        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS word_count_api (\n" +
                "    word STRING PRIMARY KEY NOT ENFORCED,\n" +
                "    cnt BIGINT\n" +
                ")");

        // 1. Write Data
        tableEnv.executeSql("CREATE TEMPORARY TABLE IF NOT EXISTS word_table_api (\n" +
                "    word STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'fields.word.length' = '1'\n" +
                ")");

        tableEnv.executeSql("INSERT INTO word_count_api SELECT word, COUNT(*) FROM word_table_api GROUP BY word");

//        env.execute();
    }
}