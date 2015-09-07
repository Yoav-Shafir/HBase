package crud.get;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class GetTryWithResourcesExample {
	static Admin admin = null;
	static TableName tableName = TableName.valueOf("Users");
	
	public static void main(String[] args) throws Exception	 {
		
		//Create the configuration.	
		Configuration configuration = HBaseConfiguration.create(); 	
		
		try (Connection connection = ConnectionFactory.createConnection(configuration)) {
			admin = connection.getAdmin();	
			if (!admin.tableExists(tableName)){
				
				// HTableDescriptor contains the details about an HBase table such as 
				// the descriptors of all the column families
				HTableDescriptor desc = new HTableDescriptor(tableName);
				
				HColumnDescriptor coldef = new HColumnDescriptor("data");
				desc.addFamily(coldef);
				admin.createTable(desc);
			}
			
			// Instantiate a new table reference.
			try (Table table = connection.getTable(tableName)) {
				
				// Create get with specific row.
				Get get = new Get(Bytes.toBytes("rowKey1"));
				
				// Add a column to the get.
				get.addColumn(Bytes.toBytes("data"), Bytes.toBytes("json"));
				
				// Retrieve row with selected columns from HBase.
				Result result = table.get(get);
				
				// Get a specific value for the given column.
				byte[] val = result.getValue(Bytes.toBytes("data"), Bytes.toBytes("json"));
				System.out.println("Value: " + Bytes.toString(val));
			}
		}
	}
}
