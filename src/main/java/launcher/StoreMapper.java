package launcher;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import com.google.gson.Gson;

import model.StoreData;

public class StoreMapper implements MapFunction<Row, StoreData> {

	@Override
	public StoreData call(Row row) throws Exception {

		StoreData storeData = new Gson().fromJson(row.getString(1), StoreData.class);
		return storeData;
	}

}
