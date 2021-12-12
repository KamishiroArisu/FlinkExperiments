package flink.experiments.length3;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

public class FlinkInsertOnly {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Input
        tableEnv.connect(new FileSystem().path(args[0]))
                .withFormat(new Csv())
                .withSchema(new Schema().field("src", DataTypes.INT())
                                        .field("dst", DataTypes.INT()))
                .createTemporaryTable("Graph");

        // Result
        tableEnv.connect(new FileSystem().path(args[1]))
                .withFormat(new Csv())
                .withSchema(new Schema().field("src", DataTypes.INT())
                                        .field("via1", DataTypes.INT())
                                        .field("via2", DataTypes.INT())
                                        .field("dst", DataTypes.INT()))
                .createTemporaryTable("Path");

        String sql = "SELECT A.src AS src, A.dst AS via1, C.src AS via2, C.dst AS dst " +
                "FROM Graph AS A, Graph AS B, Graph AS C " +
                "WHERE A.dst = B.src AND B.dst = C.src";

        tableEnv.sqlQuery(sql).executeInsert("Path");
    }
}
