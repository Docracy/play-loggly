/**
 *
 */
package com.spidertracks.loggly;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.ErrorHandler;
import org.hsqldb.Server;
import org.hsqldb.server.ServerConstants;

/**
 * Class that performs all operations on the embedded log database
 *
 * @author Todd Nine
 */
public class EmbeddedDb {


    private Connection conn;
    private ErrorHandler errorHandler;
    private String insertStatement = null;
    private String selectStatement = null;
    private String deleteStatement = null;
    public Object initializeLock = new Object();

    private final String databasePath;
    private final String hsqlConfigUrl;
    
    private boolean shutdownRequested = false;
    private final Server server;
    
    // This needs to be static
    private static final Object serverLock = new Object();

    /**
     * The embedded db
     *
     * @param dirName
     * @param logName
     * @param errorHandler
     */
    public EmbeddedDb(String dirName, String logName, ErrorHandler errorHandler) {

        databasePath = "file:" + dirName + "/" + logName + ";hsqldb.applog=1; hsqldb.lock_file=false";
        hsqlConfigUrl = "jdbc:hsqldb:" + databasePath;
        
        server = startServer();
        
        this.errorHandler = errorHandler;
        try {
            createTableAndIndex(dirName, logName);
        } catch (SQLException e) {
            errorHandler.error("Unable to create local database for log queue",
                    e, 1);
        }
    }

    /**
     * Create a new entry with the given timestamp
     *
     * @param message
     * @param time
     */
    public boolean writeEntry(String message, long time) {
        try {
            PreparedStatement ps = conn.prepareStatement(insertStatement);
            ps.setLong(1, System.nanoTime());
            ps.setString(2, message);
            ps.executeUpdate();
            ps.close();

            return true;
        } catch (SQLException e) {
            errorHandler.error("Unable to persist log message", e, 1);
        }

        return false;
    }

    /**
     * Return the next entry in the db. If one does not exist, null is returned
     *
     * @return
     */
    public List<Entry> getNext(int size) {
        List<Entry> entries = new ArrayList<Entry>();

        try {
            PreparedStatement ps = conn.prepareStatement(selectStatement);
            ps.setInt(1, size);

            ResultSet rs = ps.executeQuery();

            while (rs.next()) {

                Entry entry = new Entry();

                entry.setId(rs.getLong(1));

                entry.setMessage(rs.getString(2));
                entry.setTime(rs.getLong(3));

                entries.add(entry);

            }
            ps.close();

        } catch (SQLException e) {
            errorHandler.error("Unable to query the embedded db", e, 2);
        }

        return entries;
    }

    /**
     * Delete the entity. True if successful, false otherwise
     *
     * @param e
     * @return
     */
    public int deleteEntries(List<Entry> entries) {

        StringBuffer buffer = new StringBuffer();
        buffer.append(deleteStatement);


        for (Entry entry : entries) {
            buffer.append(entry.getId()).append(",");
        }

        buffer.setLength(buffer.length() - 1);
        buffer.append(")");


        try {

            PreparedStatement ps = conn.prepareStatement(buffer.toString());
            int count = ps.executeUpdate();
            ps.close();

            return count;
        } catch (SQLException e) {
            errorHandler.error("Unable to persist log message", e, 1);
        }

        return 0;
    }

    public boolean isInitialized() {
        return conn != null;
    }

    private Server startServer() {
        synchronized (serverLock) {
            final Server server = new Server();

            LogLog.setInternalDebugging(true);
            LogLog.setQuietMode(false);
            server.setDatabasePath(0, "testo");
            server.setDatabaseName(0, "loggly");
            server.setLogWriter(null);
            server.setErrWriter(null);
            LogLog.debug("Loggly: about to start server");
            // todo If I don't start this in a separate thread it hangs. I don't know why.
            Runnable runnable = new Runnable() {
                public void run() {
                    server.start();
                }
            };
            Thread thread = new Thread(runnable);
            thread.start();
            LogLog.debug("Loggly: server started");

            return server;            
        }
    }

    public void shutdown() {
        synchronized (serverLock) {
            try {
                
                // this is a silly wrapper, but it looks like log4j can cause shutdown()
                // to be called multiple times. (both directly and through finalize())
                if (!shutdownRequested) {
                    LogLog.debug("Loggly: shutting down database");
                    PreparedStatement ps = conn.prepareStatement("SHUTDOWN COMPACT");
                    ps.execute();
                    ps.close();
    
                    server.stop();
                    shutdownRequested = true;
                }

                while (server.getState() != ServerConstants.SERVER_STATE_SHUTDOWN) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        LogLog.warn("Interrupted waiting for shutdown", e);
                    }
                }
            } catch (SQLException e) {
                errorHandler.error("Unable to close HSQL database", e, 1);
            }
        }
    }

    /**
     * Start the database and create the table if it doesn't exist
     */
    private void createTableAndIndex(String dirName, String logName) throws SQLException {

        try {
            Class.forName("org.hsqldb.jdbcDriver");
        } catch (ClassNotFoundException e) {
            LogLog.error("Cannot start HSQL database", e);
        }

        synchronized (initializeLock) {
            LogLog.debug("Loggly: Initializing database");
            conn = DriverManager.getConnection(hsqlConfigUrl, "sa", "");
            initializeLock.notify();
        }

        insertStatement = "INSERT INTO " + logName + " (time, message) VALUES(?, ?)";
        selectStatement = "SELECT TOP ? id, message, time FROM " + logName + " ORDER BY id";
        deleteStatement = "DELETE FROM " + logName + " WHERE id in (";

        // The table may already exist if the queue is persistent.
        if (tableExists(logName)) {
            return;
        }

        Statement st = conn.createStatement();

        st.execute("CREATE CACHED TABLE " + logName
                + "(id BIGINT IDENTITY PRIMARY KEY, message LONGVARCHAR, time BIGINT)");
        st.execute("CREATE INDEX id_index ON " + logName + "(id)");
        st.execute("CREATE INDEX time_index ON " + logName + "(time)");
        st.close();

    }

    /**
     * Check if the given tablename already exists
     *
     * @param tableName
     * @return
     * @throws SQLException
     */
    private boolean tableExists(String tableName) throws SQLException {

        PreparedStatement stmt = null;
        ResultSet results = null;
        try {
            stmt = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
            results = stmt.executeQuery();
            return true; // if table does exist, no rows will ever be returned
        } catch (SQLException e) {
            return false; // if table does not exist, an exception will be
            // thrown
        } finally {
            if (results != null) {
                results.close();
			}
			if (stmt != null) {
				stmt.close();
			}
		}
	}

}
