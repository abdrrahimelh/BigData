import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.HColumnDescriptor;

public class WriteToHBase {
	private static final String TABLE_NAME = "helfani:stat_perday"; //IL FAUT CHANGER LE NAMESPACE

	public static class WriteReducer extends TableReducer<LongWritable, Text, Text> {
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// something that need to be done at start of reducer
		}


		///CAPTEUR(P?),TYPECAPTEUR,SENS,JOUR,MOIS/ANNEE,HEURE:MINUTE:SECONDE:CENTIEME,VITESSE,TYPE VEHICULE
		private Put getPutFromLine(LongWritable key ,  Text val ){
			Put put = new Put(key.toString().getBytes());
			put.addColumn(Bytes.toBytes("Data"), Bytes.toBytes("entrée"), val.toString().split(",")[0].getBytes());
			put.addColumn(Bytes.toBytes("Data"), Bytes.toBytes("sortie"), val.toString().split(",")[1].getBytes());
			return put;
		}

		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			for (Text val : values) {
				Put put = getPutFromLine(key , val);
				context.write(new Text(key.toString()), put);
			}
		}
	}

	public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
		if (admin.tableExists(table.getTableName())) {
			admin.disableTable(table.getTableName());
			admin.deleteTable(table.getTableName());
		}
		admin.createTable(table);
	}
	public static void createTable(Connection connect) {
		try {
			final Admin admin = connect.getAdmin();
			TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(TABLE_NAME));
			ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("Data".getBytes()).build();
			tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor);
			TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
			if (!admin.tableExists(TableName.valueOf(TABLE_NAME))) {
				admin.createTable(tableDescriptor);
			}
			admin.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(-1);
		}
	}

	public static void main (String[] args) throws Exception {
		Configuration conf =  HBaseConfiguration.create();
		Job job = Job.getInstance(conf, "Write to HBase example");
		job.setJarByClass(WriteToHBase.class);
		//create the table (sequential part)
		Connection connection = ConnectionFactory.createConnection(conf);
		createTable(connection);
		//input from HDFS file
		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		//output to an HBase table
		TableMapReduceUtil.initTableReducerJob(TABLE_NAME, WriteReducer.class, job);
		job.waitForCompletion(true);
	}
}
