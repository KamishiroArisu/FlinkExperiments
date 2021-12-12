package flink.experiments.length3;

import flink.experiments.utils.ResourcesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class FlinkInsertDelete {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        String createTableGraph = ResourcesUtil.readResourceFile("length3/Graph-InsertDelete-DDL.sql");
        tableEnv.executeSql(createTableGraph);

        String query = ResourcesUtil.readResourceFile("length3/Query-Length3.sql");
        Table result = tableEnv.sqlQuery(query);
        tableEnv.toRetractStream(result, Row.class).print();
        env.execute();
    }
}
