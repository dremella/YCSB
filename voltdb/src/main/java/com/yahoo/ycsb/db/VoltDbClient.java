/**
 * VoltDB client binding for YCSB.
 *
 * Created by Dheeraj Remella on 5/15/2013.
 *
 *
 */

package com.yahoo.ycsb.db;

import java.io.IOException;

import java.net.UnknownHostException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;

/**
 * VoltDB client for YCSB framework.
 *
 * 
 * @author dremella
 */
public class VoltDbClient extends DB {

    /** Used to include a field in a response. */
    protected static final Integer INCLUDE = Integer.valueOf(1);

    /** A singleton VoltDB Client instance. */
    private static Client client;

    /** The database to access. */
    private static String database;

    /** Count the number of times initialized to teardown on the last {@link #cleanup()}. */
    private static final AtomicInteger initCount = new AtomicInteger(0);
    
    public VoltDbClient() {
        database = "localhost";
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void init() throws DBException {
        initCount.incrementAndGet();
        synchronized (INCLUDE) {
            if (client != null) {
                return;
            }

            // initialize VoltDB client instance
            Properties props = getProperties();
            String url = "localhost";
            
            try {
                ClientConfig config = new ClientConfig();
                config.setMaxTransactionsPerSecond(2000);
                
                client = ClientFactory.createClient(config);
                client.createConnection(database);
            } catch (UnknownHostException uhe) {
                uhe.printStackTrace();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    @Override
    public void cleanup() throws DBException {
        if (initCount.decrementAndGet() <= 0) {
            try {
                client.drain();
                client.close();
            }
            catch (Exception e1) {
                e1.printStackTrace();
                return;
            }
        }
    }

    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int delete(String table, String key) {
        
        try {
            client.callProcedure("DeleteUser", key);
            return 0;
        } catch (Exception ex) {
            ex.printStackTrace();
            return 1;
        }
        /*
        try {
            String deleteSPName = "delete" + table;
            client.callProcedure(deleteStoredProcName, key);
        }
        catch (Exception e) {
            System.err.println(e.toString());
            return 1;
        }
         */
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int insert(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            client.callProcedure("INSERTUSER", key, values.get("field0").toString(),values.get("field1").toString(),values.get("field2").toString(),
                                 values.get("field3").toString(),values.get("field4").toString(),values.get("field5").toString(),
                                 values.get("field6").toString(),values.get("field7").toString(),values.get("field8").toString(),
                                 values.get("field9").toString());
            
            return 0;

        }
        catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }

    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    @Override
    @SuppressWarnings("unchecked")
    public int read(String table, String key, Set<String> fields,
            HashMap<String, ByteIterator> result) {
        try {
            ClientResponse response = client.callProcedure("SelectUser", key);
            VoltTable resultTable = response.getResults()[0];
            int rowCount = resultTable.getRowCount();
            int columnCount = resultTable.getColumnCount();
            result.clear();
            if (rowCount == 1) {
                VoltTableRow row = resultTable.fetchRow(0);
                for (int i = 0 ; i < columnCount ; i++) {
                    String field = resultTable.getColumnName(i);
                    String value = row.getString(i);
                    result.put(field, new StringByteIterator(value));
                }
            } else {
                return 1;
            }
            return 0;
        } catch (Exception ex) {
            ex.printStackTrace();
            return 1;
        }
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int update(String table, String key,
            HashMap<String, ByteIterator> values) {
        try {
            ClientResponse response = client.callProcedure("SelectUser", key);
            VoltTable resultTable = response.getResults()[0];
            int rowCount = resultTable.getRowCount();
            int columnCount = resultTable.getColumnCount();
            if (rowCount == 1) {
                VoltTableRow row = resultTable.fetchRow(0);
                if (values.get("field0") == null) values.put("field0", new StringByteIterator(row.getString("FIELD0")));
                if (values.get("field1") == null) values.put("field1", new StringByteIterator(row.getString("FIELD1")));
                if (values.get("field2") == null) values.put("field2", new StringByteIterator(row.getString("FIELD2")));
                if (values.get("field3") == null) values.put("field3", new StringByteIterator(row.getString("FIELD3")));
                if (values.get("field4") == null) values.put("field4", new StringByteIterator(row.getString("FIELD4")));
                if (values.get("field5") == null) values.put("field5", new StringByteIterator(row.getString("FIELD5")));
                if (values.get("field6") == null) values.put("field6", new StringByteIterator(row.getString("FIELD6")));
                if (values.get("field7") == null) values.put("field7", new StringByteIterator(row.getString("FIELD7")));
                if (values.get("field8") == null) values.put("field8", new StringByteIterator(row.getString("FIELD8")));
                if (values.get("field9") == null) values.put("field9", new StringByteIterator(row.getString("FIELD9")));
            }
            
            client.callProcedure("UpdateUser", key, values.get("field0").toString(), values.get("field1").toString(), values.get("field2").toString(), values.get("field3").toString(),
                                 values.get("field4").toString(), values.get("field5").toString(), values.get("field6").toString(), values.get("field7").toString(), values.get("field8").toString(),
                                 values.get("field9").toString());
            return 0;
        } catch (Exception ex) {
            ex.printStackTrace();
            return 1;
        }
    }

    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    @Override
    public int scan(String table, String startkey, int recordcount,
            Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        try {
            ClientResponse response = client.callProcedure("ScanUsers", startkey, recordcount);
            VoltTable resultTable = response.getResults()[0];
            int rowCount = resultTable.getRowCount();
            System.out.println("Number of rows scanned: " + rowCount);
            int columnCount = resultTable.getColumnCount();
            result.clear();
            for (int j = 0 ; j < rowCount ; j++) {
                VoltTableRow row = resultTable.fetchRow(j);
                HashMap<String, ByteIterator> rowMap = new HashMap<String, ByteIterator>();
                for (int i = 0 ; i < columnCount ; i++) {
                    String field = resultTable.getColumnName(i);
                    if (field.equalsIgnoreCase("key")) continue;
                    String value = row.getString(i);
                    rowMap.put(field, new StringByteIterator(value));
                }
                result.add(rowMap);
            }

            return 0;
        } catch (Exception ex) {
            ex.printStackTrace();
            return 1;
        }

    }

}
