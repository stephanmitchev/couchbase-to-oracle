import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.jdbc.OraclePreparedStatement;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

/**
 * @author smitchev
 *
 *
 */
public class CouchbaseSync {

	static public void setupTables(String uniqueName) throws SQLException {

		Connection conn = DriverManager
				.getConnection("jdbc:default:connection:");

		// Get the current model
		JSONDataModel model = new JSONDataModel(conn, uniqueName);

		// Create the tables
		if (model.tables.size() > 0) {

			for (DataTable table : model.tables.values()) {

				String sql = "CREATE TABLE \"TBL_" + table.name.toUpperCase()
						+ "\" (";

				for (int i = 0; i < table.rows.size(); i++) {

					DataRow row = table.rows.get(i);

					sql += " \""
							+ row.values
									.get(table.indexOfColumn("shortColumn"))
									.toString().toUpperCase()
							+ "\" "
							+ CouchbaseModeler
									.getOracleTypeFromJavaType(row.values.get(
											table.indexOfColumn("datatype"))
											.toString());

					if (i < table.rows.size() - 1) {
						sql += ", ";
					}
				}

				sql += ")";

				sendDBMSOutput(
						"Creating table using: "
								+ sql, conn);
								
				PreparedStatement psCreate = conn.prepareStatement(sql);
				try {
					psCreate.executeUpdate();

				} catch (SQLException ex) {
					sendDBMSOutput(
							"Could not insert model definition Cought an exception: "
									+ ex.getMessage(), conn);
				} finally {
					psCreate.close();
				}
				// No commits on DDL
				// conn.commit();
			}
		}

		conn.close();
	}

	static public void synchronize(String uniqueName, String rootModelTableName,
			String timeColumn) throws SQLException, ClassNotFoundException {

		String rootTable = "TBL_" + rootModelTableName;
		
		Connection conn = DriverManager
				.getConnection("jdbc:default:connection:");

		// Get the current model
		JSONDataModel model = new JSONDataModel(conn, uniqueName);
		sendDBMSOutput("Model loaded...", conn);

		// Find the last sync time form the existing tables
		PreparedStatement psMax = conn.prepareStatement("SELECT \""
				+ timeColumn + "\" FROM \"" + rootTable + "\" ORDER BY \""
				+ timeColumn + "\" DESC");
		ResultSet resLastSync = psMax.executeQuery();
		
		DateTime lastSync = new DateTime(0);
		try {
			resLastSync.next();
			lastSync = resLastSync.getTimestamp(1) != null ? new DateTime(
					resLastSync.getTimestamp(1).getTime()) : new DateTime(0);
					
		}
		catch (Exception e){
			sendDBMSOutput(e.getMessage(), conn);
			
		}

		sendDBMSOutput("Last Sync time: " + lastSync.toString(), conn);

		resLastSync.close();
		psMax.close();


		// Get the view details
		// Get view details
		sendDBMSOutput("Loading view...", conn);
		PreparedStatement psCount = conn
				.prepareStatement("SELECT * FROM COUCHBASE_VIEWS$ WHERE UNIQUENAME = ?");
		psCount.setString(1, uniqueName);
		ResultSet res = psCount.executeQuery();

		String url = "";
		String bucket = "";
		String password = "";
		String ddoc = "";
		String view = "";

		if (res.next()) {
			url = res.getString(2);
			bucket = res.getString(3);
			password = res.getString(4);
			ddoc = res.getString(5);
			view = res.getString(6);
		} else {
			res.close();
			psCount.close();
			sendDBMSOutput("Could not find a view defined by uniqueName", conn);

			return;

		}
		res.close();
		psCount.close();
		
		
		

		// Get the Docs and syncronize
		sendDBMSOutput("Getting docs to sync", conn);

		String userPassword = bucket + ":" + password;
		String encoding = new sun.misc.BASE64Encoder().encode(userPassword
				.getBytes());

		InputStream inView = null;
		try {

			HttpURLConnection httpV = (HttpURLConnection) new URL(url
					+ bucket
					+ "/_design/"
					+ ddoc
					+ "/_view/"
					+ view
					+ "?startkey=" + URLEncoder.encode("\""+lastSync.toString("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")+"\"","UTF-8") + "&connection_timeout=60000"
					).openConnection();
			
			sendDBMSOutput("Firing " + httpV.getURL().toString(), conn);

			httpV.setRequestProperty("Authorization", "Basic " + encoding);
			inView = httpV.getInputStream();

			JSONTokener tV = new JSONTokener(inView);
			JSONObject rootV = new JSONObject(tV);

			JSONArray rows = rootV.getJSONArray("rows");
			
			sendDBMSOutput("Got " + rows.length() + " results", conn);

			for (int i = 0; i < rows.length(); i++) {
				String id = rootV.getJSONArray("rows").getJSONObject(i)
						.getString("id");
			
			
			
				// Delete all documents with the new IDs
				// When we synchronize we will start from this key inclusively
				if (id != null && !id.equals("") && model.tables.size() > 0) {
	
					for (DataTable table : model.tables.values()) {
	
						String sql = "DELETE FROM \"TBL_" + table.name.toUpperCase()
								+ "\" WHERE DOC_ID = '"+id+"'" ;
	
						CallableStatement psDelete = conn.prepareCall(sql);
						try {
							psDelete.executeUpdate();
						} catch (SQLException ex) {
							sendDBMSOutput(
									"Could not delete updated record using '"+sql+"': "
											+ ex.getMessage(), conn);
						} finally {
							psDelete.close();
							conn.commit();
						}
					}
				}
				
			}
			
			
			for (int i = 0; i < rows.length(); i++) {
				
				
				
				InputStream in = null;

				String id = rootV.getJSONArray("rows").getJSONObject(i)
						.getString("id");

				
				HttpURLConnection http = (HttpURLConnection) new URL(url
						+ bucket + "/" + id).openConnection();
				http.setRequestProperty("Authorization", "Basic " + encoding);

				in = http.getInputStream();

				JSONTokener t = new JSONTokener(in);
				JSONObject rootJS = new JSONObject(t);
				JsonObject root = new JsonObject(rootJS.toString());

				for (String tableName : model.tables.keySet()) {

					DataTable table = model.tables.get(tableName);

					// Process the child table yielding multiple records in v
					if (table.rows.size() > 2
							&& table.rows.get(1).values
									.get(table.indexOfColumn("xpath"))
									.toString().startsWith("#")) {

						//sendDBMSOutput("Processing child table", conn);
						processChildTable(table, root, id, conn);

					}
					// Process the main document yielding a single record to v
					else {
						
						//sendDBMSOutput("Processing main table", conn);
						processMainTable(table, root, id, conn);

					}
					//conn.commit();
				}

				in.close();
				
				
			}

			conn.commit();
			
			conn.close();
			
		} catch (Exception ex) {
			sendDBMSOutput("General Exception: " + ex.getMessage(), conn);
			
			
		} finally {
			if (inView != null) {
				try {
					inView.close();
				} catch (IOException e) {

				}
			}

			if (!conn.isClosed()) {
				conn.commit();
				conn.close();
			}
		}

	}

	static private void processMainTable(DataTable table, JsonObject root,
			String id, Connection conn) throws SQLException {

		String sql = "INSERT INTO \"TBL_" + table.name.toUpperCase()
				+ "\" VALUES (";

		sendDBMSOutput("Processing ID:" + id + " SQL:" + sql + "...", conn);
		for (int i = 0; i < table.rows.size(); i++) {
			sql += " ?" + (i < table.rows.size() - 1 ? "," : "");
		}

		sql += ")";
		
		//sendDBMSOutput("Processing ID:" + id + " SQL:" + sql, conn);
		

		PreparedStatement psInsert;
		
		try {
			psInsert = conn.prepareStatement(sql);
					
			for (int i = 0; i < table.rows.size(); i++) {

				DataRow row = table.rows.get(i);
				// Special cases
				if (row.values.get(2).toString().equals("#ID")) {
					((OraclePreparedStatement)psInsert).setFormOfUse(1, OraclePreparedStatement.FORM_NCHAR);
					psInsert.setString(i+1, id);
					
				}
				else 
				{
					addParameter(psInsert, i+1, root, row.values.get(1).toString(),row.values.get(2).toString(), conn);
				}
					
			}
			
			psInsert.execute();
			psInsert.close();
			
			
		} catch (SQLException e) {
			sendDBMSOutput("Main Table Exception: " + e.getMessage() + e.getNextException(), conn);
			
		}

	}

	static private void processChildTable(DataTable modelTable,
			JsonObject root, String id, Connection conn)
			throws ClassNotFoundException, SQLException {

		// The counters will track how many levels of nesting we have
		int counters = 0;

		// We will first represent the entire output table written out as XPaths
		// We will use the DataColumn as it has storage for xpath in the name
		// property
		// and a Datatype in the datatype property
		List<List<DataColumn>> xPathRows = new ArrayList<List<DataColumn>>();

		// a single row of data
		List<DataColumn> xPathRow = new ArrayList<DataColumn>();

		String sql = "INSERT INTO \"TBL_" + modelTable.name.toUpperCase()
				+ "\" VALUES (";
		sendDBMSOutput("Child SQL for id: " + id + "," + sql + "...", conn); 
		// Collect counters and initial row XPaths
		for (int i = 0; i < modelTable.rows.size(); i++) {

			DataRow row = modelTable.rows.get(i);
			
			String xPath = row.values.get(modelTable.indexOfColumn("xpath"))
					.toString();
			Class<?> dataType = Class.forName(row.values.get(
					modelTable.indexOfColumn("datatype")).toString());

			if (xPath.equals("#ID")) {
				// Nothing for now
			} else if (xPath.startsWith("#")) {
				counters++;
			}

			xPathRow.add(new DataColumn(xPath, dataType));

			sql += " ?" + (i < modelTable.rows.size() - 1 ? "," : "");
						
		}

		sql += ")";

		//sendDBMSOutput("Child SQL: " + sql, conn);
		
		// Add the initial Row
		xPathRows.add(xPathRow);

		// Iteratively expand each row based on the indexes found
		for (int i = 0; i < counters; i++) {
			xPathRows = expandRows(root, i, xPathRows, modelTable);
		}

		// Iterate over the expanded rows. They should contain
		// full XPaths to be used to obtain the values for each cell
		for (List<DataColumn> row : xPathRows) {

			// Set up the result row
			
			PreparedStatement psInsert;
			try {
				psInsert = conn.prepareStatement(sql);

				// Iterate over each cell
				for (int i = 0; i < row.size(); i++) {

					DataColumn cell = row.get(i);
					//sendDBMSOutput("Adding param: " + (i+1) +", " + cell.name, conn);
					
					// Handle special case of DOC_ID
					if (cell.name.equals("#ID")) {
						psInsert.setString(i+1, id);

					}
					// Handle special case of child table indexes
					else if (cell.name.startsWith("#")) {
						// Obtain the nesting index from the XPath by looking at
						// the first index found
						int counterIndex = Integer.parseInt(cell.name
								.substring(1));

						Pattern p = Pattern.compile("\\[([0-9]+)\\]");
						Matcher m = p.matcher(row.get(row.size() - 1).name);

						long index = m.find() ? Long.parseLong(m
								.group(counterIndex + 1)) : 0;

						psInsert.setLong(i+1, index);
					} else {
						addParameter(psInsert, i+1, root,
								cell.dataType.getName(), cell.name, conn);

					}

				}

				psInsert.executeUpdate();
				psInsert.close();

			} catch (SQLException e) {
				sendDBMSOutput("Exception in processChildTable: " + e.getMessage(), conn);
				
			}

		}

	}

	static private List<List<DataColumn>> expandRows(JsonObject root,
			int counterIndex, List<List<DataColumn>> xPathRows,
			DataTable modelTable) {

		// Create a new result
		List<List<DataColumn>> result = new ArrayList<List<DataColumn>>();

		// For every row that requires expansion
		for (List<DataColumn> row : xPathRows) {
			// Last column must contain a counter as the indexes and ids are in
			// the beginning
			String xPath = row.get(row.size() - 1).name;

			// Find the n-th ocurrence of an index where n == counterIndex
			int cIdx = 0;
			for (int i = 0; i <= counterIndex; i++) {
				cIdx = xPath.indexOf("[", cIdx + 1);
			}

			// Obtain the real count within the document that we are currently
			// writing out
			int resultRowCount = root.getArray(xPath.substring(0, cIdx))
					.length();

			// For every entry found
			for (int i = 0; i < resultRowCount; i++) {

				// Create a new expanded row
				List<DataColumn> newRow = new ArrayList<DataColumn>(row);

				// and for every cell of that row
				for (int j = 0; j < newRow.size(); j++) {
					// Skipping the special cases
					if (!newRow.get(j).name.startsWith("#")) {
						// Create a sequential index within the XPath expression
						DataColumn newCell = new DataColumn(
								newRow.get(j).name.substring(0, cIdx)
										+ "["
										+ i
										+ "]"
										+ newRow.get(j).name
												.substring(cIdx + 3),
								newRow.get(j).dataType);
						newRow.set(j, newCell);
					}
				}

				// Add the expanded row back to the result set
				result.add(newRow);
			}
		}

		return result;
	}

	static private void addParameter(PreparedStatement ps, int idx,
			JsonObject root, String typeName, String xPath, Connection conn) throws SQLException {
 
		if (root.get(xPath).data == null) {
			ps.setNull(idx, java.sql.Types.NULL);
		}
		else if (typeName.equals(Boolean.class.getName()))
			ps.setInt(idx, root.getBool(xPath) ? 1 : 0);
		else if (typeName.equals(Long.class.getName()))
			try {ps.setLong(idx, root.getLong(xPath));} catch(NumberFormatException e) 
			{	ps.setNull(idx, java.sql.Types.NULL);
				sendDBMSOutput("NumberFormatException for getLong, offending value is '" + root.getString(xPath) + ",", conn);

			}
		else if (typeName.equals(Double.class.getName()))
			try {ps.setDouble(idx, root.getDouble(xPath));} catch(NumberFormatException e) 
			{	ps.setNull(idx, java.sql.Types.NULL);
				sendDBMSOutput("NumberFormatException for getDouble, offending value is '" + root.getString(xPath) + ",", conn);
			}
		else if (typeName.equals(Date.class.getName())) {
			String dateStr = root.getString(xPath);
			java.sql.Timestamp sqlTS = null;

			try {
				DateTime dt = DateTimeFormat.forPattern(
						"yyyy-MM-dd'T'HH:mm:ss.SSSZZ").parseDateTime(dateStr);
				sqlTS = new java.sql.Timestamp(dt.getMillis());
			} catch (Exception ex) {
			}

			if (sqlTS == null) {
				try {
					DateTime dt = DateTimeFormat.forPattern("MM/dd/yyyy")
							.parseDateTime(dateStr);
					sqlTS = new java.sql.Timestamp(dt.getMillis());
				} catch (Exception ex1) {
				}
			}
			
			if (dateStr != null && dateStr.length() > 0 && sqlTS == null)
			{
				sendDBMSOutput("Exception for parsing date string, offending value is '" + root.getString(xPath) + ",", conn);
			}

			if (sqlTS != null)
				ps.setTimestamp(idx, sqlTS);
			else
				ps.setNull(idx, java.sql.Types.NULL);
		}

		else if (typeName.equals(String.class.getName())) {
			String s = root.getString(xPath);
			if (s == null) {
				ps.setNull(idx, java.sql.Types.NULL);
			}
			else {
				ps.setString(idx, ""+s.substring(0, Math.min(s.length(), 2000)));
			}
		}
	
	}

	static private void sendDBMSOutput(String msg, Connection conn)
			throws SQLException {
		CallableStatement cs = conn.prepareCall("{call DBMS_OUTPUT.PUT_LINE('"
				+ msg.replace('\'', ' ') + "')}");
		cs.executeUpdate();
		cs.close();
	}

}
