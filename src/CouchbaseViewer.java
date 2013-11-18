import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import oracle.CartridgeServices.ContextManager;
import oracle.CartridgeServices.CountException;
import oracle.CartridgeServices.InvalidKeyException;
import oracle.sql.ARRAY;
import oracle.sql.ArrayDescriptor;
import oracle.sql.STRUCT;
import oracle.sql.StructDescriptor;
import oracle.sql.TIMESTAMP;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;



/**
 * @author smitchev
 *
 */
public class CouchbaseViewer implements SQLData  {
	
	private BigDecimal key;
	
	final static BigDecimal SUCCESS = new BigDecimal(0);
	final static BigDecimal ERROR = new BigDecimal(1);

	String sql_type;
	public String getSQLTypeName() throws SQLException 
	{
		return sql_type;
	}


	public void readSQL(SQLInput stream, String typeName) throws SQLException {
		sql_type = typeName;
	    key = stream.readBigDecimal();		
	}

	public void writeSQL(SQLOutput stream) throws SQLException {
		stream.writeBigDecimal(key);
		
	}
	
	
	// type methods implementing ODCITable interface

	static public BigDecimal ODCITableStart(STRUCT[] sctx, String uniqueName, String tableName)
			throws SQLException {
		
		Connection conn = DriverManager.getConnection("jdbc:default:connection:");
		
		// create a stored context and store the result set in it
	    StoredCtx ctx=new StoredCtx();
	    
	    // Get view details
 		PreparedStatement psCount = conn.prepareStatement("SELECT * FROM COUCHBASE_VIEWS$ WHERE UNIQUENAME = ?");
 		psCount.setString(1, uniqueName);
 		ResultSet res = psCount.executeQuery();
 		
 		if (res.next()) {
 			ctx.url = res.getString(2);
 			ctx.bucket = res.getString(3);
 			ctx.password = res.getString(4);
 			ctx.ddoc = res.getString(5);
 			ctx.view = res.getString(6);
 		}
 		else {
 			res.close();
 			psCount.close();
 			return ERROR;
 			
 		} 
 		res.close();
 		psCount.close();
 		
 		// Get the IDs
 		String userPassword = ctx.bucket + ":" + ctx.password;
		String encoding = new sun.misc.BASE64Encoder().encode(userPassword.getBytes());
		
		InputStream inView = null;
	    try {
 			
 			HttpURLConnection http = (HttpURLConnection)new URL(ctx.url+ctx.bucket+"/_design/"+ctx.ddoc+"/_view/"+ctx.view+"?connection_timeout=60000").openConnection();
			http.setRequestProperty("Authorization", "Basic " + encoding);
			inView = http.getInputStream();

			JSONTokener t = new JSONTokener(inView);
			JSONObject root = new JSONObject(t);
			
			JSONArray rows = root.getJSONArray("rows");
			
			for (int i = 0; i < rows.length(); i++) {
				ctx.ids.add(root.getJSONArray("rows").getJSONObject(i).getString("id"));
			}
			sendDBMSOutput("Found "+ctx.ids.size()+" documents...", conn);
			
		
 		}
 		catch(IOException ex){}
 		finally { 
 			if (inView != null)
 				try {
 					inView.close();
 				} catch (IOException e) {
 					
 				}
 		}
	    
 		ctx.uniqueName = uniqueName;
 		ctx.tableName = tableName;
 		ctx.model = new JSONDataModel(conn, ctx.uniqueName);
 		sendDBMSOutput("Model loaded...", conn);
 			
 		
	    // register stored context with cartridge services
	    int key = 0;
	    try {
	      key = ContextManager.setContext(ctx);
	    } catch (CountException ce) {
	      return ERROR;
	    }
	    // create a ViewImpl instance and store the key in it
	    Object[] impAttr = new Object[1];
	    impAttr[0] = new BigDecimal(key); 
	    StructDescriptor sd = new StructDescriptor("T"+tableName.toUpperCase()+"_VIEWIMPL",conn);
	    sctx[0] = new STRUCT(sd,conn,impAttr);
	      
		return SUCCESS;
	}

	public BigDecimal ODCITableFetch(BigDecimal nrows, ARRAY[] outSet)
			throws SQLException, ClassNotFoundException, MalformedURLException, IOException {
		Connection conn = DriverManager
				.getConnection("jdbc:default:connection:");

		sendDBMSOutput("Fetching...", conn);
		
		// retrieve stored context using the key
	    StoredCtx ctx;
	    try {
	      ctx=(StoredCtx)ContextManager.getContext(key.intValue());
	    } catch (InvalidKeyException ik ) {
	      return ERROR;
	    }

	    
		Vector<Object> v = new Vector<Object>(nrows.intValue());
	    
		StructDescriptor rowDesc = StructDescriptor.createDescriptor(
				"T"+ctx.tableName.toUpperCase()+"_ROW", conn);
		
		String userPassword = ctx.bucket + ":" + ctx.password;
		String encoding = new sun.misc.BASE64Encoder().encode(userPassword.getBytes());
		
			
		int limit = 0;
		
		while (ctx.currentIdx < ctx.ids.size() && limit++ < 500){
			
			InputStream in = null;
			
			HttpURLConnection http = (HttpURLConnection)new URL(ctx.url+ctx.bucket+"/"+ctx.ids.get(ctx.currentIdx)).openConnection();
 			http.setRequestProperty("Authorization", "Basic " + encoding);
			
		
 			in = http.getInputStream();
 			
 			JSONTokener t = new JSONTokener(in);
 			JSONObject rootJS = new JSONObject(t);
 			JsonObject root = new JsonObject(rootJS.toString());
 			//sendDBMSOutput("Processing id: " + ctx.ids.get(ctx.currentIdx) + "and looking at table "+ctx.tableName, conn);
			
 			 
 			if (ctx.model.tables.containsKey(ctx.tableName)) {
 				
 				DataTable table = ctx.model.tables.get(ctx.tableName);
 			
	 			// Process the child table yielding multiple records in v
	 			if (table.rows.size() > 2 &&table.rows.get(1).values.get(table.indexOfColumn("xpath")).toString().startsWith("#")) {
	 				//sendDBMSOutput("Processing nested table in "+ctx.ids.get(ctx.currentIdx)+"...", conn);
	 				
	 				List<Object[]> records = processChildTable(table, root, ctx.ids.get(ctx.currentIdx));
	 				
	 				for(Object[] record : records) {
	 					v.add((Object) new STRUCT(rowDesc, conn, record));
	 				}
	 				
	 			}
	            // Process the main document yielding a single record to v
	 			else {
	 				
	 				try {
	 					Object[] record = processMainTable(table, root, ctx.ids.get(ctx.currentIdx));
		 				v.add((Object) new STRUCT(rowDesc, conn, record));
		 			}
	 				catch (Exception ex) {
	 					
	 				}
	 			}
 			}
 			
 			in.close();
		
			ctx.currentIdx++;
			
		}

 		
		// create the output ARRAY using the vector
		Object out_arr[] = v.toArray();
		ArrayDescriptor ad = new ArrayDescriptor("T"+ctx.tableName.toUpperCase()+"_ROWSET", conn);
		outSet[0] = new ARRAY(ad, conn, out_arr);
		
		sendDBMSOutput("Sending rowset of: "+ad.getName(), conn);
			

		return SUCCESS;
	}

			
	public Object[] processMainTable(DataTable table, JsonObject root, String id) {
			
		Object[] result = new Object[table.rows.size()];
		
		for (int i = 0; i < table.rows.size(); i++) {
			
			DataRow row = table.rows.get(i);
			
			// Special cases
			if (row.values.get(2).toString().equals("#ID")) {
				result[i] = id;
				continue;
			}
			
			try {
			
				result[i] = writeValue(root, row.values.get(1).toString(), row.values.get(2).toString());
			}
			catch(Exception ex) { }
			
		}
		
		return result;
	}


	private Object writeValue(JsonObject root, String typeName, String xPath) {

		Object value = null;
	
		if (root.get(xPath).data == null)
			return null;
		
		if (typeName.equals(Boolean.class.getName()))
			value = root.getBool(xPath) ? 1 : 0;
		else if (typeName.equals(Long.class.getName()))
			value = root.getLong(xPath);
		else if (typeName.equals(Double.class.getName()))
			value = root.getDouble(xPath);
		else if (typeName.equals(Date.class.getName())) {
			String dateStr = root.getString(xPath);
			java.sql.Timestamp sqlTS = null;
			
			try	{
				DateTime dt = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").parseDateTime(dateStr);
				sqlTS = new java.sql.Timestamp(dt.getMillis());
			}
			catch(Exception ex) {}
			
			if (sqlTS == null) {
				try {
					DateTime dt = DateTimeFormat.forPattern("MM/dd/yyyy").parseDateTime(dateStr);
					sqlTS = new java.sql.Timestamp(dt.getMillis());
				}
				catch(Exception ex1) {}
			}
			
			if (sqlTS != null)
				value = TIMESTAMP.toBytes(sqlTS);
		}
		
		else if (typeName.equals(String.class.getName())) {
			String s = root.getString(xPath);
			value = (Object)s.substring(0, Math.min(s.length(), 2000));
		}
		return value;
	}


	public BigDecimal ODCITableClose() throws SQLException {

		return SUCCESS;
	}
	
	static private void sendDBMSOutput(String msg, Connection conn) throws SQLException {
		
		CallableStatement cs = conn.prepareCall("{call DBMS_OUTPUT.PUT_LINE('"+msg.replace('\'', ' ')+"')}");
		cs.executeUpdate();
		cs.close();
	}
	
	
	public List<Object[]> processChildTable(DataTable modelTable, JsonObject root, String id) throws ClassNotFoundException {
		
		List<Object[]> results = new ArrayList<Object[]>();
		
		// The counters will track how many levels of nesting we have
        int counters = 0;

		// We will first represent the entire output table written out as XPaths
		// We will use the DataColumn as it has storage for xpath in the name property
		// and a Datatype in the datatype property
        List<List<DataColumn>> xPathRows = new ArrayList<List<DataColumn>>();

        // a single row of data
        List<DataColumn> xPathRow = new ArrayList<DataColumn>();
		
        // Collect counters and initial row XPaths
        for (DataRow row : modelTable.rows)
        {
            
        	String xPath = row.values.get(modelTable.indexOfColumn("xpath")).toString();
        	Class<?> dataType = Class.forName(row.values.get(modelTable.indexOfColumn("datatype")).toString());

            if (xPath.equals("#ID"))
            {
                // Nothing for now
            }
            else if (xPath.startsWith("#"))
            {
                counters++;
            }

            xPathRow.add(new DataColumn(xPath, dataType));
            
        }
        
        // Add the initial Row
        xPathRows.add(xPathRow);

        
        // Iteratively expand each row based on the indexes found
        for (int i = 0; i < counters; i++)
        {
            xPathRows = expandRows(root, i, xPathRows, modelTable);
        }
        

        // Iterate over the expanded rows. They should contain 
        // full XPaths to be used to obtain the values for each cell
        for (List<DataColumn> row : xPathRows) {
        	
        	// Set up the result row
        	Object[] res = new Object[row.size()];
        	
        	// Iterate over each cell
        	for (int i = 0; i < row.size(); i++) {
        		
        		DataColumn cell = row.get(i);
        		 
        		// Handle special case of DOC_ID
                if (cell.name.equals("#ID"))
                {
                    res[i] = id;
                    
                }
                // Handle special case of child table indexes
                else if (cell.name.startsWith("#"))
                {
                    // Obtain the nesting index from the XPath by looking at the first index found
                    int counterIndex = Integer.parseInt(cell.name.substring(1));
                    
                    Pattern p = Pattern.compile("\\[([0-9]+)\\]");
                    Matcher m = p.matcher(row.get(row.size() - 1).name);
                    
                    long index = m.find() ? Long.parseLong(m.group(counterIndex + 1)) : 0;
                    
                    res[i] = index;
                }
                else
                {
                	res[i] = writeValue(root, cell.dataType.getName(), cell.name);
                }
        		
        	}
        	
        	
        	results.add(res);
        }
        
		
		
		
		return results;
	}


	private List<List<DataColumn>> expandRows(JsonObject root, int counterIndex,
			List<List<DataColumn>> xPathRows, DataTable modelTable) {
		
		// Create a new result
        List<List<DataColumn>> result = new ArrayList<List<DataColumn>>();

        // For every row that requires expansion
        for (List<DataColumn> row : xPathRows)
        {
            // Last column must contain a counter as the indexes and ids are in the beginning
        	String xPath = row.get(row.size() - 1).name;
        	
            // Find the n-th ocurrence of an index where n == counterIndex
            int cIdx = 0;
            for (int i = 0; i <= counterIndex; i++)
            {
                cIdx = xPath.indexOf("[", cIdx + 1);
            }

            // Obtain the real count within the document that we are currently writing out
            int resultRowCount = root.getArray(xPath.substring(0, cIdx)).length();
            
            // For every entry found
            for (int i = 0; i < resultRowCount; i++) {

                // Create a new expanded row
                List<DataColumn> newRow = new ArrayList<DataColumn>(row);

                // and for every cell of that row
                for (int j = 0; j < newRow.size(); j++)
                {
                    // Skipping the special cases
                    if (!newRow.get(j).name.startsWith("#"))
                    {
                        // Create a sequential index within the XPath expression
                        DataColumn newCell = new DataColumn(newRow.get(j).name.substring(0, cIdx) + "[" + i + "]" + newRow.get(j).name.substring(cIdx + 3), newRow.get(j).dataType); 
                    	newRow.set(j, newCell);
                    }
                }

                // Add the expanded row back to the result set
                result.add(newRow);
            }
        }
		
        
        return result;
	}


}
