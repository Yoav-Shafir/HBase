package crud.put;

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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class PutListExample {
	
	static Admin admin = null;
	static TableName tableName = TableName.valueOf("Users");
	
	public static void main(String[] args) throws Exception {
		Configuration configuration = HBaseConfiguration.create();
		
		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			admin = connection.getAdmin();	
			if (admin.tableExists(tableName)){
				admin.disableTable(tableName);
				admin.deleteTable(tableName);
			}

			// HTableDescriptor contains the details about an HBase table such as 
			// the descriptors of all the column families
			HTableDescriptor desc = new HTableDescriptor(tableName);
			HColumnDescriptor coldef = new HColumnDescriptor("data");
			desc.addFamily(coldef);
			admin.createTable(desc);

			// Instantiate a new table reference.
			// Instantiate a new client.
			try (Table table = connection.getTable(tableName)) {
				
				// Create a list that holds the Put instances.
				List<Put> puts = new ArrayList<Put>(); 
				
				// Create put with specific row.
				Put put1 = new Put(Bytes.toBytes("row1"));
				// Add a column, whose name is "data:json", to the put.
				put1.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"),
						Bytes.toBytes("{\"fname\":\"Eiyar\",\"lname\":\"Goldman\",\"email\":\"email@gmail.com\"}")); 
				puts.add(put1);
				
				Put put2 = new Put(Bytes.toBytes("row2"));
				put2.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"),
						Bytes.toBytes("{\"fname\":\"Zohari\",\"lname\":\"Shafir-Goldman\",\"email\":\"email@gmail.com\"}")); 
				puts.add(put2);
				
				Put put3 = new Put(Bytes.toBytes("row2"));
				put3.addColumn(Bytes.toBytes("data"), Bytes.toBytes("another_qualifier"),
						Bytes.toBytes("another_value")); 
				puts.add(put3);
				
				// Store multiple rows with columns into HBase.
				table.put(puts); 
			}
		}
	}
}
