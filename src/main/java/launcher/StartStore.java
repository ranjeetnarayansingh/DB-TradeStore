package launcher;

import static org.apache.spark.sql.functions.col;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import com.google.gson.Gson;

import model.StoreData;

public class StartStore {
	public static String tableName = "TradeStore";
	public static String dburl = "jdbc:mysql://localhost:3306/Test";
	public static String username = "root";
	public static String password = "root";
	public static String driverClass = "org.mariadb.jdbc.Driver";
	public static String brokers = "localhost:9092";
	public static String topic = "testone";

	public static void main(String[] args) throws TimeoutException, StreamingQueryException {
		String isRun = "YES";
		Scanner scanner = new Scanner(System.in);

		System.out.println("please install kafka and MYSQL in then if its already running then enter yes");
		isRun = scanner.next();
		System.out.println("before Running the program you have to create MYSQL Data Table");
		System.out.println(
				"create table Command : create table TradeStore(tradestore_id INT NOT NULL AUTO_INCREMENT, trade_id VARCHAR(100) NOT NULL UNIQUE, trade_version VARCHAR(100) NOT NULL, counter_party_id VARCHAR(40) NOT NULL, book_id VARCHAR(40), maturity_date VARCHAR(40), created_date DATETIME DEFAULT CURRENT_TIMESTAMP, Expired VARCHAR(40), PRIMARY KEY ( tradestore_id ) );");
		System.out.println("if you created the Table then enter yes");
		if (isRun.equalsIgnoreCase("YES") && scanner.next().equalsIgnoreCase("YES")) {

			System.out.println("please Provide DB URL");
			dburl = scanner.next();

			System.out.println("please Provide DB username");
			username = scanner.next();

			System.out.println("please Provide DB password");
			password = scanner.next();

			System.out.println("please Provide KAFKA brokers");
			brokers = scanner.next();

			System.out.println("please Provide KAFKA topic");
			topic = scanner.next();
			SparkSession sparkSession = SparkSession.builder().appName("TradeStore").master("local").getOrCreate();

			Dataset<Row> rawData = sparkSession.readStream().format("kafka").option("kafka.bootstrap.servers", brokers)
					.option("subscribe", topic).load().selectExpr("cast(value as string) as value");

			Dataset<StoreData> mapData = rawData
					.map((org.apache.spark.api.java.function.MapFunction<Row, StoreData>) x -> {
						StoreData storeData = new Gson().fromJson(x.getString(0), StoreData.class);
						return storeData;
					}, Encoders.bean(StoreData.class));

			StreamingQuery start = mapData.writeStream().foreachBatch((partData, id) -> {

				Dataset<Row> jdbcData = getJDBCData(sparkSession).persist();

				Dataset<StoreData> persistData = partData.persist();

				Dataset<Row> allData = persistData.join(jdbcData, col("tradeId").equalTo(col("trade_Id")), "left");
				Dataset<Row> sqlData = allData
						.selectExpr("*",
								"(case when version<trade_version then true when date_format(maturity_date,'dd/MM/yyyy')<=date(now()) then true else false end) as filtercon")
						.where("filtercon=false");

				updateJDBCData(sqlData.selectExpr("tradeId as trade_id", "version as trade_version",
						"counterPartyId as counter_party_id", "bookId as  book_id", "maturityDate as maturity_date",
						"'N' as Expired"));

				putJDBCData(persistData.join(jdbcData, col("tradeId").equalTo(col("trade_Id")), "left")
						.where("tradestore_id is null").dropDuplicates(), SaveMode.Append);

				updateJDBCData(jdbcData.filter("date_format(maturity_date,'dd/MM/yyyy')<=date(now())")
						.selectExpr("*", "'Y' as ExpiredUpdate").drop("Expired")
						.withColumnRenamed("ExpiredUpdate", "Expired"));

			}).trigger(Trigger.ProcessingTime(5l, TimeUnit.SECONDS)).start();
			start.awaitTermination();
		} else {
			System.out.println("Please Install KAFKA and MYSQL on Your System then Run is again");
		}
	}

	private static Dataset<Row> getJDBCData(SparkSession sparkSession) {
		Dataset<Row> jdbcData = sparkSession.read().jdbc(dburl, tableName, setJDBCProperties());
		return jdbcData;
	}

	public static Properties setJDBCProperties() {
		Properties connectionProperties = new Properties();
		connectionProperties.put("user", username);
		connectionProperties.put("password", password);
		connectionProperties.put("Driver", driverClass);
		return connectionProperties;
	}

	private static void updateJDBCData(Dataset<Row> sqlData) throws ClassNotFoundException, SQLException {

		List<Row> collectAsList = sqlData.collectAsList();
		Class.forName(driverClass);

		try (Connection dbConnection = DriverManager.getConnection(dburl, username, password);) {
			PreparedStatement preparedStatement = dbConnection.prepareStatement(
					"update TradeStore set trade_version=?,counter_party_id=?,book_id=?,maturity_date=?,Expired=? where trade_id=?");

			String[] updateColumnName = { "trade_version", "counter_party_id", "book_id", "maturity_date", "Expired",
					"trade_id" };

			for (Row row : collectAsList) {
				if (updateColumnName != null && updateColumnName.length != 0) {
					for (int index = 1; index <= updateColumnName.length; index++) {
						Object value = row.get(row.fieldIndex(updateColumnName[index - 1]));
						preparedStatement.setObject(index, value);
					}
				}
				preparedStatement.addBatch();

			}
			preparedStatement.executeBatch();
		}

	}

	private static void putJDBCData(Dataset<Row> sqlData, SaveMode mode) {
		sqlData.show(false);
		sqlData.selectExpr("tradeId as trade_id", "version as trade_version", "counterPartyId as counter_party_id",
				"bookId as  book_id", "maturityDate as maturity_date", "'N' as Expired").write().mode(mode)
				.jdbc(dburl, tableName, setJDBCProperties());
	}

}