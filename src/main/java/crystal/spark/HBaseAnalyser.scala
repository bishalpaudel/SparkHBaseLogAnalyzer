package crystal.spark

import org.apache.hadoop.hbase.client.{ConnectionFactory, Delete, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}

/**
  * Created by cloudera on 10/23/16.
  */
object HBaseAnalyser {

  def main(args: Array[String]) {
    val conf = HBaseConfiguration create
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin()

    val tableName = TableName.valueOf("AccessLog")

    if(admin.tableExists(tableName))
    {
      if (admin.isTableEnabled(tableName)) {
        admin.disableTable(tableName)
      }
      admin.deleteTable(tableName)
    }

    val hTableDesc = new HTableDescriptor(tableName);
    hTableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("url")))
    hTableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("method")));
    hTableDesc.addFamily(new HColumnDescriptor(Bytes.toBytes("count")));
    admin.createTable(hTableDesc);

    val table = connection.getTable(tableName);

    /* Create new Records */
    var i = 0
    for (i <- 1 to 100) {
      val p = new Put(Bytes.toBytes("url_" + i));
      p.addColumn(Bytes.toBytes("url"), Bytes.toBytes("method"), Bytes.toBytes("method_" + i))
      p.addColumn(Bytes.toBytes("url"), Bytes.toBytes("count"), Bytes.toBytes("count_" + i))
      table.put(p)
    }

    /* Update records */
    for (i <- 50 to 100) {
      val p = new Put(Bytes.toBytes("url_" + i));
      p.addColumn(Bytes.toBytes("url"), Bytes.toBytes("method"), Bytes.toBytes("UPDATED_method_" + i))
      table.put(p)
    }
    /* Delete some records */
    for (i <- 90 to 100) {
      val delete = new Delete(Bytes.toBytes("url_" + i));
      /* Delete all versions of the column */
      delete.addColumns(Bytes.toBytes("url"), Bytes.toBytes("method"))
      table.delete(delete)
    }

    table.close
    connection.close
  }
}
