import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * @author smitchev
 *
 */
public class CouchbaseModeler {

	static public void setup() throws SQLException {
		
		Connection conn = DriverManager.getConnection("jdbc:default:connection:");

		// Setup the VIEWS table
		Statement stmt = conn.createStatement();
		
		String sql = "CREATE TABLE COUCHBASE_VIEWS$ " +
                "(UNIQUENAME VARCHAR2(2000) NOT NULL, " +
                " URL VARCHAR(2000) NOT NULL, " + 
                " BUCKET VARCHAR(255) NOT NULL, " + 
                " PASS VARCHAR(255) NOT NULL, " + 
                " DDOC VARCHAR(255) NOT NULL, " + 
                " VIEWNAME VARCHAR(255) NOT NULL, " + 
                " PRIMARY KEY ( UNIQUENAME ))"; 
		
		try {
			stmt.execute(sql);
		}
		catch(SQLException ex) {
			sendDBMSOutput("Table COUCHBASE_VIEWS$ already exists. Please drop it and then try running setup() again. Got: " + ex.getMessage(), conn);
		}
		finally {
			stmt.close();
		}
		
		// Setup the MODELS table
		stmt = conn.createStatement();
		
		sql = "CREATE TABLE COUCHBASE_MODELS$ " +
                "(UNIQUENAME VARCHAR2(2000) NOT NULL, " +
                " TABLE_NAME VARCHAR(2000) NOT NULL, " + 
                " IDX INTEGER NOT NULL, " + 
                " INFERRED_NAME VARCHAR(255) NOT NULL, " + 
                " DATATYPE VARCHAR(255) NOT NULL, " + 
                " XPATH VARCHAR(2000) NOT NULL, " + 
                " ORACLE_NAME VARCHAR(2000) NOT NULL, " + 
                " VALID INTEGER NOT NULL, " + 
                " PRIMARY KEY ( UNIQUENAME, TABLE_NAME, INFERRED_NAME ))"; 
		
		try {
			stmt.execute(sql);
		}
		catch(SQLException ex) {
			sendDBMSOutput("Table COUCHBASE_MODELS$ already exists. Please drop it and then try running setup() again. Got: " + ex.getMessage(), conn);
		}
		finally {
			stmt.close();
		}
		
	}

	static public void registerView(String uniqueName, String url, String bucket, String password, String ddoc, String view) throws SQLException {
		
		Connection conn = DriverManager.getConnection("jdbc:default:connection:");
		
		// Setup the VIEWS table
		PreparedStatement psCount = conn.prepareStatement("SELECT COUNT(*) AS CNT FROM COUCHBASE_VIEWS$ WHERE UNIQUENAME = ?");
		psCount.setString(1, uniqueName);
		ResultSet resCount = psCount.executeQuery();
		resCount.next();
		
		if (resCount.getLong(1) > 0) {
			sendDBMSOutput("Couchbase view with the name '"+uniqueName+"' is already registered. Please remove it and try again.", conn);
			resCount.close();
			psCount.close();
			return;
		}
		resCount.close();
		psCount.close();
		
	
		
		PreparedStatement psInsert = conn.prepareStatement("INSERT INTO COUCHBASE_VIEWS$ VALUES (?, ?, ?, ?, ?, ?)");
		psInsert.setString(1, uniqueName);
		psInsert.setString(2, url);
		psInsert.setString(3, bucket);
		psInsert.setString(4, password);
		psInsert.setString(5, ddoc);
		psInsert.setString(6, view);
		try {
			psInsert.executeUpdate();
			conn.commit();
			sendDBMSOutput("View '"+uniqueName+"' has been registered. Proceeed in modeling the view.", conn);
		}
		catch(SQLException ex) {
			sendDBMSOutput("Could not register the view. Cought an exception: " + ex.getMessage(), conn);
		}
		finally {
			psInsert.close();
		}
		
		
		
	}

	static public void modelView(String uniqueName, String rootXPath, String tableNameXPath, BigDecimal sampleSize) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:default:connection:");
		
		// Clean existing model
		PreparedStatement psDelete = conn.prepareStatement("DELETE FROM COUCHBASE_MODELS$ WHERE UNIQUENAME = ?");
		psDelete.setString(1, uniqueName);
		try {
			psDelete.executeUpdate();
			conn.commit();
			sendDBMSOutput("Deleted the existing model for " + uniqueName, conn);
		}
		catch(SQLException ex) {
			sendDBMSOutput("Could not delete model. Cought an exception: " + ex.getMessage(), conn);
			return;
		}
		finally {
			psDelete.close();
		}
		
		modelViewAdditive(uniqueName, rootXPath, tableNameXPath, sampleSize);

		conn.close();
		
		
	}

	static public void modelViewAdditive(String uniqueName, String rootXPath, String tableNameXPath, BigDecimal sampleSize) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:default:connection:");
		
		// Clean input
		rootXPath = rootXPath == null ? "" : rootXPath;
		
		
		// Get view details
		PreparedStatement psCount = conn.prepareStatement("SELECT * FROM COUCHBASE_VIEWS$ WHERE UNIQUENAME = ?");
		psCount.setString(1, uniqueName);
		ResultSet res = psCount.executeQuery();
		
		String url = "";
		String bucket = "";
		String pass = "";
		String ddoc = "";
		String view = "";
		
		if (res.next()) {
			url = res.getString(2);
			bucket = res.getString(3);
			pass = res.getString(4);
			ddoc = res.getString(5);
			view = res.getString(6);
		}
		else {
			sendDBMSOutput("Could find view "+uniqueName+".", conn);
			res.close();
			psCount.close();
			return;
			
		}
		res.close();
		psCount.close();
		
		// Sample the view and create the model
		JSONDataModel model = new JSONDataModel();
		
		String userPassword = bucket + ":" + pass;
		String encoding = new sun.misc.BASE64Encoder().encode(userPassword.getBytes());
		
		String logMessages = "";
		
		InputStream inView = null;
		InputStream inDoc = null;
		try {
			HttpURLConnection http = (HttpURLConnection)new URL(url+bucket+"/_design/"+ddoc+"/_view/"+view+"?connection_timeout=60000&limit="+sampleSize+"&skip=0").openConnection();
			http.setRequestProperty("Authorization", "Basic " + encoding);
			inView = http.getInputStream();

			JSONTokener t = new JSONTokener(inView);
			JSONObject root = new JSONObject(t);
			
			List<String> ids = new ArrayList<String>();
			
			JSONArray rows = root.getJSONArray("rows");
			sendDBMSOutput("Sampling from "+rows.length()+" documents.", conn);
			
			for (int i = 0; i < rows.length(); i++) {
				ids.add(root.getJSONArray("rows").getJSONObject(i).getString("id"));
			}
			
			for(String id : ids) {
				HttpURLConnection http1 = (HttpURLConnection)new URL(url+bucket+"/"+id).openConnection();
				http1.setRequestProperty("Authorization", "Basic " + encoding);
				inDoc = http1.getInputStream();
			
				t = new JSONTokener(inDoc);
				root = new JSONObject(t);
				
				try {
					JSONDataModel newModel = new JSONDataModel();
					newModel.generateModel(root.toString(), rootXPath, tableNameXPath, logMessages);
					model = model.mergeWith(newModel);
				} catch (Exception e) {
					sendDBMSOutput("Could not generate model. Got: " + e.getMessage(), conn);
					return;
				}
			}
			
		}
		catch(IOException ex){}
		finally { 
			if (inView != null)
				try {
					inView.close();
				} catch (IOException e) {
					
				}
			if (inDoc != null)
				try {
					inDoc.close();
				} catch (IOException e) {
					
				}
		}
		
		sendDBMSOutput("Model generated. "+model.tables.size()+" tables.", conn);
		
		int invalidRecords = 0;
		
		if (model.tables.size() > 0) {
			
			for(DataTable table : model.tables.values()) {
				int idx = 0;
				for(DataRow row : table.rows) {
					
					PreparedStatement psInsert = conn.prepareStatement("INSERT INTO COUCHBASE_MODELS$ VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
					psInsert.setString(1, uniqueName);
					psInsert.setString(2, table.name);
					psInsert.setInt(3, idx);
					psInsert.setString(4, row.values.get(0).toString());
					psInsert.setString(5, row.values.get(1).toString());
					psInsert.setString(6, row.values.get(2).toString());
					psInsert.setString(7, row.values.get(3).toString());
					invalidRecords += row.values.get(3).toString().length() > 30 ? 1 : 0;
					psInsert.setInt(8, row.values.get(3).toString().length() > 30 ? 0 : 1);
					try {
						psInsert.executeUpdate();
						
					}
					catch(SQLException ex) {
						sendDBMSOutput("Could not insert model definition Cought an exception: " + ex.getMessage(), conn);
					}
					finally
					{
						psInsert.close();
						idx++;
					}
					
				}
				
			}
			
			conn.commit();
			
			
		}
		
		sendDBMSOutput("Model saved successfully. There are "+invalidRecords+" invalid records in the model. Check that all table names and oracle_column entries are 30 chars or below ", conn);
		sendDBMSOutput("You should run 'SELECT * FROM COUCHBASE_MODELS$ WHERE LENGTH(TABLE_NAME) > 24 OR LENGTH(ORACLE_NAME) > 30' and make sure that there are no results.", conn);
		
		conn.close();
	}
	
	static public String getOracleTypeFromJavaType(String typeName) {
		
		String res = null;
		
		if (typeName.equals(Boolean.class.getName()))
			res = "NUMBER(1)";
		else if (typeName.equals(Long.class.getName()))
			res = "NUMBER(20)";
		else if (typeName.equals(Double.class.getName()))
			res = "NUMBER(24,6)";
		else if (typeName.equals(Date.class.getName()))
			res = "TIMESTAMP(6)";
		else
			res = "NVARCHAR2(2000)";
				
		return res;
	}
	
	static public void createViewTypes(String uniqueName) throws SQLException {
		Connection conn = DriverManager.getConnection("jdbc:default:connection:");
		
		// Load the model
		JSONDataModel model = new JSONDataModel(conn, uniqueName);
		String sql = "";
		Statement stmt = null;
		
		for(String tableName : model.tables.keySet()) {
		
			try {
				stmt = conn.createStatement();
				stmt.execute("DROP VIEW V"+tableName.toUpperCase());
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				stmt = conn.createStatement();
				stmt.execute("DROP FUNCTION T"+tableName.toUpperCase()+"_VIEW");
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				stmt = conn.createStatement();
				stmt.execute("DROP TYPE T"+tableName.toUpperCase()+"_VIEWIMPL");
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				stmt = conn.createStatement();
				stmt.execute("DROP TYPE T"+tableName.toUpperCase()+"_ROWSET");
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				stmt = conn.createStatement();
				stmt.execute("DROP TYPE T"+tableName.toUpperCase()+"_ROW");
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				sql = "CREATE OR REPLACE TYPE T"+tableName.toUpperCase()+"_ROW AS OBJECT (";
				for (int i = 0; i < model.tables.get(tableName).rows.size(); i++) {
					DataRow row = model.tables.get(tableName).rows.get(i);
					sql += " \"" + row.values.get(3) + "\" " + getOracleTypeFromJavaType(row.values.get(1).toString()) + ",";
				}
				sql = sql.substring(0, sql.length() - 1);
				sql += ")";
				stmt = conn.createStatement();
				stmt.execute(sql);
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				stmt = conn.createStatement();
				stmt.execute("CREATE OR REPLACE TYPE T"+tableName.toUpperCase()+"_ROWSET AS TABLE OF T"+tableName.toUpperCase()+"_ROW");
				stmt.close();
				
				stmt = conn.createStatement();
				sql = "CREATE TYPE T"+tableName.toUpperCase()+"_VIEWIMPL AS OBJECT " +
						"( " +
						" " +
						"  key NUMBER, " +
						"   " +
						"  STATIC FUNCTION ODCITableStart(sctx OUT T"+tableName.toUpperCase()+"_VIEWIMPL, UNIQUENAME IN NVARCHAR2, TABLENAME IN NVARCHAR2) " +
						"    RETURN NUMBER " +
						"    AS LANGUAGE JAVA " +
						"    NAME 'CouchbaseViewer.ODCITableStart(oracle.sql.STRUCT[], java.lang.String, java.lang.String)  " +
						"return java.math.BigDecimal', " +
						" " +
						"  MEMBER FUNCTION ODCITableFetch(self IN OUT T"+tableName.toUpperCase()+"_VIEWIMPL, nrows IN NUMBER, " +
						"                                 outSet OUT T"+tableName.toUpperCase()+"_ROWSET) RETURN NUMBER " +
						"    AS LANGUAGE JAVA " +
						"    NAME 'CouchbaseViewer.ODCITableFetch(java.math.BigDecimal,  " +
						"oracle.sql.ARRAY[]) return java.math.BigDecimal', " +
						" " +
						"  MEMBER FUNCTION ODCITableClose(self IN T"+tableName.toUpperCase()+"_VIEWIMPL) RETURN NUMBER " +
						"    AS LANGUAGE JAVA " +
						"    NAME 'CouchbaseViewer.ODCITableClose() return java.math.BigDecimal' " +
						" " +
						"); ";
				stmt.execute(sql);
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				stmt = conn.createStatement();
				stmt.execute("CREATE FUNCTION T"+tableName.toUpperCase()+"_VIEW(UNIQUENAME IN NVARCHAR2, TABLENAME IN NVARCHAR2) RETURN T"+tableName.toUpperCase()+"_ROWSET PIPELINED USING T"+tableName.toUpperCase()+"_VIEWIMPL;");
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
			try {
				stmt = conn.createStatement();
				stmt.execute("CREATE VIEW V"+tableName.toUpperCase()+" AS SELECT * FROM TABLE(T_54_313_422_VIEW('"+uniqueName+"', '"+tableName.toUpperCase()+"'))");
			} 
			catch(Exception e) {}
			finally { stmt.close(); }
			
		}
		
		conn.close();
	}
	

	static private void sendDBMSOutput(String msg, Connection conn) throws SQLException {
		CallableStatement cs = conn.prepareCall("{call DBMS_OUTPUT.PUT_LINE('"+msg.replace('\'', ' ')+"')}");
		cs.executeUpdate();
		cs.close();
	}
	
}
