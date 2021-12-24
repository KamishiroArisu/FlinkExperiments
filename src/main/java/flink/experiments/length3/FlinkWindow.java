package flink.experiments.length3;

import flink.experiments.utils.ResourcesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createTableGraph = ResourcesUtil.readResourceFile("length3/Graph-Window-DDL.sql");
        tableEnv.executeSql(createTableGraph);

        String createViewGraphWindowed = ResourcesUtil.readResourceFile("length3/Graph-Window-Hop.sql");
        tableEnv.executeSql(createViewGraphWindowed);

        String createTablePath = ResourcesUtil.readResourceFile("length3/Path-Window-DDL.sql");
        tableEnv.executeSql(createTablePath);

        String query = ResourcesUtil.readResourceFile("length3/Query-Window-Length3.sql");
        tableEnv.sqlQuery(query).executeInsert("Path");
    }
}
