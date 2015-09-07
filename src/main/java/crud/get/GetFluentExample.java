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

public class GetFluentExample {
	
	static Admin admin = null;
	static TableName tableName = TableName.valueOf("Users");
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		
		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			admin = connection.getAdmin();	
			
			// delete old table if exist
			if (admin.tableExists(tableName)){
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}
			
			// HTableDescriptor contains the details about an HBase table such as 
			// the descriptors of all the column families
			HTableDescriptor desc = new HTableDescriptor(tableName);
			
			// An HColumnDescriptor contains information about a column family such as the number of versions, compression settings,
			// etc. It is used as input when creating a table or adding a column.
			HColumnDescriptor coldef1 = new HColumnDescriptor("data");
			HColumnDescriptor coldef2 = new HColumnDescriptor("another_colfamily");
			desc.addFamily(coldef1);
			desc.addFamily(coldef2);
			admin.createTable(desc);
			
			// Instantiate a new table reference.
			// Instantiate a new client.
			try (Table table = connection.getTable(tableName)) {
				
				// Create put with specific row.
				Put put = new Put(Bytes.toBytes("row1"));
				
				// Add a column, whose name is "data:json", to the put.
				put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"),
						Bytes.toBytes("{\"fname\":\"Eiyar\",\"lname\":\"Goldman\",\"email\":\"email@gmail.com\"}"));
				
				// Add a column, whose name is "another_colfamily:another_qualifier", to the put.
				put.addColumn(Bytes.toBytes("another_colfamily"), Bytes.toBytes("another_qualifier"),
						Bytes.toBytes("another_value"));
				
				// Store row with column into the HBase table.
				table.put(put);
				
				
				
				//-- Get the data --//
				
				List<Get> gets = new ArrayList<Get>();
				Get get1 = new Get(Bytes.toBytes("row1"));
				
				// Get up to the specified number of versions of each column.
				get1.setMaxVersions();
				gets.add(get1);
				
				Result[] results = table.get(gets);
				for (Result result : results) {
					for (Cell cell : result.rawCells()) {
						
						System.out.println("Cell: " + cell + ", Value: " + 
								
								// Bytes.toString:
								// arg1 - Presumed UTF-8 encoded byte array.
							    // arg2 - offset into array
							    // arg3 - length of utf-8 sequence
								Bytes.toString(cell.getValueArray(),
										cell.getValueOffset(), cell.getValueLength()));
					}
				}
				
				System.out.println("----------------------------------------");
				
				// Create a new get using the fluent interface.
				Get get2 = new Get(Bytes.toBytes("row1")) 
					.setId("GetFluentExample") // This method allows you to set an identifier on an operation.
					.setMaxVersions() // Get all available versions.
					//.setTimeStamp(1) // Get versions of columns with the specified timestamp.
					.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"))
					.addFamily(Bytes.toBytes("another_colfamily"));
				
				Result result = table.get(get2);
				System.out.println("Result: " + result);
			}
		}
	}
}
