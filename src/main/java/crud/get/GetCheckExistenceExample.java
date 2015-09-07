package crud.get;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.Cell;

public class GetCheckExistenceExample {

	static Admin admin = null;
	static TableName tableName = TableName.valueOf("Users");

	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();

		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			admin = connection.getAdmin();

			// delete old table if exist.
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}

			// HTableDescriptor contains the details about an HBase table such
			// as the descriptors of all the column families.
			HTableDescriptor desc = new HTableDescriptor(tableName);

			// An HColumnDescriptor contains information about a column family
			// such as the number of versions, compression settings,
			// etc. It is used as input when creating a table or adding a
			// column.
			HColumnDescriptor coldef1 = new HColumnDescriptor("data");
			HColumnDescriptor coldef2 = new HColumnDescriptor("another_colfamily");
			desc.addFamily(coldef1);
			desc.addFamily(coldef2);
			admin.createTable(desc);

			// Instantiate a new table reference.
			// Instantiate a new client.
			try (Table table = connection.getTable(tableName)) {

				List<Put> puts = new ArrayList<Put>();

				// row1.
				// Create put with specific row.
				Put put1 = new Put(Bytes.toBytes("row1"));
				// Add a column, whose name is "data:json", to the put.
				put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"),
						Bytes.toBytes("{\"fname\":\"Yoav\",\"lname\":\"Shafir\",\"email\":\"email@gmail.com\"}"));
				puts.add(put1);

				// row2.
				Put put2 = new Put(Bytes.toBytes("row2"));
				put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"),
						Bytes.toBytes("{\"fname\":\"Eiyar\",\"lname\":\"Goldman\",\"email\":\"email@gmail.com\"}"));
				puts.add(put2);

				// row2.
				Put put3 = new Put(Bytes.toBytes("row2"));
				put3.addColumn(Bytes.toBytes("data"), Bytes.toBytes("another_qualifier"),
						Bytes.toBytes("another_value"));
				puts.add(put3);
				
				// Insert two rows into the table.
				table.put(puts);
				
				// -- Get the data --//
				
				
				// create a Get object for row key - row2.	
				Get get1 = new Get(Bytes.toBytes("row2"));
				get1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"));
				
				// get1 Only checks for existence of data, but do not return any of it.
				get1.setCheckExistenceOnly(true);
				
				// execute the check.
				Result result1 = table.get(get1);

				byte[] val = result1.getValue(Bytes.toBytes("data"), Bytes.toBytes("json"));
				
				// Exists is "true", while no cell was actually returned.
				System.out.println("Get 1 Exists: " + result1.getExists());
				System.out.println("Get 1 Size: " + result1.size()); 
				System.out.println("Get 1 Value: " + Bytes.toString(val));

				System.out.println("----------------------------------------");

				Get get2 = new Get(Bytes.toBytes("row2"));
				
				// Check for an entire family to exist.
				get2.addFamily(Bytes.toBytes("data")); 
				get2.setCheckExistenceOnly(true);
				Result result2 = table.get(get2);

				System.out.println("Get 2 Exists: " + result2.getExists());
				System.out.println("Get 2 Size: " + result2.size());

				System.out.println("----------------------------------------");

				Get get3 = new Get(Bytes.toBytes("row2"));

				// Check for a non-existent column.
				get3.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qual9999"));
				get3.setCheckExistenceOnly(true);
				Result result3 = table.get(get3);

				System.out.println("Get 3 Exists: " + result3.getExists());
				System.out.println("Get 3 Size: " + result3.size());

				System.out.println("----------------------------------------");

				Get get4 = new Get(Bytes.toBytes("row2"));

				// Check for an existent, and non-existent column.
				get4.addColumn(Bytes.toBytes("data"), Bytes.toBytes("qual9999"));
				get4.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"));
				get4.setCheckExistenceOnly(true);
				Result result4 = table.get(get4);

				// Exists is "true" because some data exists.
				System.out.println("Get 4 Exists: " + result4.getExists());
				System.out.println("Get 4 Size: " + result4.size());
			}
		}
	}
}
