package flinkapi.read;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class ReadFromTable {

    public static void main(String[] args) throws Exception {
//        //加载自定义参数
//        String propertiesFilePath = "/Users/leicq/flink_spaces/quickstart/src/main/resources/flink-conf.properties";
//        ParameterTool parameters = ParameterTool.fromPropertiesFile(propertiesFilePath);

        // 定义一个配置 import org.apache.flink.configuration.Configuration;包下
        Configuration configuration = new Configuration();
//        // 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8082);

        // create environments of both APIs
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

//        env.getConfig().setGlobalJobParameters(parameters);
//        // checkpoint 缺省是禁用的，需要主动打开
//        env.enableCheckpointing(1000);

        // create paimon catalog
        tableEnv.executeSql("CREATE CATALOG my_catalog WITH ('type' = 'paimon', 'warehouse'='file:///Users/leicq/share_dir/paimon')");
        tableEnv.executeSql("USE CATALOG my_catalog");

        // convert to DataStream
        //Table table = tableEnv.sqlQuery("SELECT * FROM sink_paimon_table");
        Table table = tableEnv.sqlQuery("select * from word_count_api");
        DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

        // use this datastream
        dataStream.executeAndCollect().forEachRemaining(System.out::println);

    }
}