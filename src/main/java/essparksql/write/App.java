package essparksql.write;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").appName("appname").getOrCreate();

        Dataset<Row> dfRawData = sparkSession.read().format("csv").option("header", true).option("delimiter", ",")
                .load("src/main/java/essparksql/write/dataset/netflix_titles.csv");

        dfRawData.write().format("org.elasticsearch.spark.sql").option("es.nodes", "127.0.0.1")
                .option("es.mapping.id", "show_id").option("es.port", "9200").option("es.resource", "testspark/_doc")
                .mode("overwrite").save();

    }
}
