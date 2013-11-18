import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;

    public class JSONDataModel extends DataSet
    {

        /// <summary>
        /// Finds duplicated values within the model for a given table annd within a given column
        /// </summary>
        /// <param name="tableName">The table to look into</param>
        /// <param name="columnName">The column name to inspect</param>
        /// <returns>A list of DataRow Objects that contains the rows that collide with other rows over the selected column</returns>
        public List<DataRow> findCollisions(String tableName, String columnName)
        {
            List<DataRow> result = new ArrayList<DataRow>();

            Map<String, Integer> collisions = new HashMap<String, Integer>();
            
            if (this.tables.containsKey(tableName) && this.tables.get(tableName).indexOfColumn(columnName) >= 0)
            {

                for (DataRow row : this.tables.get(tableName).rows)
                {
                    String data = row.values.get(this.tables.get(tableName).indexOfColumn(columnName)).toString();

                    if (collisions.containsKey(data))
                    {
                        collisions.put(data, collisions.get(data)+1);

                        if (collisions.get(data) >= 1)
                        {
                            result.add(row);
                        }
                    }
                    else
                    {
                        collisions.put(data, 0);
                    }
                }
            }



            return result;
        }

        /// <summary>
        /// Constructs an empty model for internal use only
        /// </summary>
        public JSONDataModel()
        {

        }

        /// <summary>
        /// Deserializes a model from an XML representation
        /// </summary>
        /// <param name="xml"></param>
        public JSONDataModel(Connection conn, String uniqueName)
        {
        	try {
		    	// Get the distinct tables
				PreparedStatement psDistinct = conn.prepareStatement("SELECT * FROM COUCHBASE_MODELS$ WHERE UNIQUENAME = ? ORDER BY TABLE_NAME, IDX");
				psDistinct.setString(1, uniqueName);
				ResultSet res = psDistinct.executeQuery();
				
				while (res.next()) {
					
					String tableName = res.getString(2);
					DataRow row = new DataRow(res.getString(4),  res.getString(5),  res.getString(6), res.getString(7));
					
					if (!tables.containsKey(tableName)) {
						DataTable table = new DataTable(tableName);
		                table.columns.add(new DataColumn("column", String.class));
		                table.columns.add(new DataColumn("datatype", String.class));
		                table.columns.add(new DataColumn("xpath", String.class));
		                table.columns.add(new DataColumn("shortColumn", String.class));
		                tables.put(tableName, table);
					}
					
					tables.get(tableName).rows.add(row);
				}
				res.close();
				psDistinct.close();
        	}
        	catch (SQLException e) {}
    		
        }

        /// <summary>
        /// Constructs the model from a given JSON document
        /// </summary>
        /// <param name="json">THe JSON document to convert</param>
        /// <param name="rootXPath">The XPath expression that points to the root from where the DataSet will be generated</param>
        /// <param name="tableNameXPath">The XPath expression that points to the name of the main parent table</param>
        /// <param name="logMessages">Log messages from the generation process</param>
        /// <returns></returns>
        public JSONDataModel(String json, String rootXPath, String tableNameXPath, String logMessages) throws Exception
        {
            generateModel(json, rootXPath, tableNameXPath, logMessages);
        }

        /// <summary>
        /// Creates a copy of the current data model
        /// </summary>
        /// <returns>A copy of the current data model</returns>
        public JSONDataModel Copy()
        {
            JSONDataModel result = new JSONDataModel();
        	
            for (DataTable t : tables.values())  {
            	
            	result.tables.put(t.name, t.clone());
            }
        	
            return result;
        }

        /// <summary>
        /// Computes the most general type between the two inputs
        /// </summary>
        /// <param name="type1">String representation of the first datatype</param>
        /// <param name="type2">String representation of the second datatype</param>
        /// <returns>The datatype that could hold values from both input datatypes</returns>
        protected static String getTypePrecedence(String t1, String t2)
        {
        	Class<?> type1 = null;
        	Class<?> type2 = null;
        	
            // if we have an invalid type, return String
            try
            {
            	type1 = Class.forName(t1);
            	type2 = Class.forName(t2);
            }
            catch (ClassNotFoundException ex)
            {
                return String.class.getName();
            }

            // if types are the same, return the first one
            if (type1.equals(type2))
            {
                return type1.getName();
            }

            // Anything paired with a String results in a String
            if (type1.equals(String.class) || type2.equals(String.class))
            {
                return String.class.getName();
            }

            // DateTime pared with anything else results in a String
            if (!type1.equals(type2) && (type1.equals(Date.class) || type2.equals(Date.class)))
            {
                return String.class.getName();
            }

            // Order of precendence in the numeric types
            List<Class<?>> precedence = new ArrayList<Class<?>>();
            precedence.add(Boolean.class);
            precedence.add(Long.class);
            precedence.add(Double.class);

            // Return the order of precedence
            if (precedence.indexOf(type1) > precedence.indexOf(type2))
            {
                return type1.getName();
            }
            else
            {
                return type2.getName();
            }

        }



        /// <summary>
        /// Performs a deep merge between two model datasets by comparing column definition rows by 
        /// their first element, and enforcing type precedence over the second elements
        /// </summary>
        /// <param name="ds1">First Dataset</param>
        /// <param name="model">Model to merge with</param>
        /// <returns>A merged vercion of both datasets. The merged version prefers the first dataset's entries
        /// for column indexes greater than 1</returns>
        public JSONDataModel mergeWith(JSONDataModel model)
        {
            // The result will begin as a copy of the first dataset
            JSONDataModel result = this.Copy();

            // Compare tables
            for (DataTable table2 : model.tables.values())
            {
            	
            	// Add the table if missing
                if (!result.tables.containsKey(table2.name))
                {
                    result.tables.put(table2.name, table2.clone());
                    
                }

                // Cache the table from the first dataset
                DataTable table1 = result.tables.get(table2.name);

                // Compare the rows and perform the merge
                for (DataRow row2 : table2.rows)
                {

                    // Find the row by comparing the first column which holds the column name of the output
                    int rowIndex = -1;
                    for (int i = 0; i < table1.rows.size(); i++)
                    {
                        if (row2.values.get(0).equals(table1.rows.get(i).values.get(0)))
                        {
                            rowIndex = i;
                            break;
                        }
                    }

                    // If the row exists, merge the datatype stored in the second column
                    if (rowIndex >= 0)
                    {
                    	table1.rows.get(rowIndex).values.set(1, getTypePrecedence(table1.rows.get(rowIndex).values.get(1).toString(), row2.values.get(1).toString()));
                    }
                    // otherwise copy the row
                    else
                    {
                        table1.rows.add(row2);
                    }
                }
            }

            return result;
        }

        /// <summary>
        /// Generates a descriptive relational model from a sample JSON file
        /// </summary>
        /// <param name="json">THe JSON document to convert</param>
        /// <param name="rootXPath">The XPath expression that points to the root dictionary from where the DataSet will be generated</param>
        /// <param name="tableNameXPath">The XPath expression that points to the name of the main parent table</param>
        /// <param name="logMessages">Log messages from the generation process</param>
        /// <returns>A DataSet that contains a schema representation infered from the sample document.
        /// Tables within the DataSet map to output tables. Each table contains three columns: column, datatype, and xpath.
        /// Column holds the output column name. Datatype is a String representation of the datatype infered from the value.
        /// Xpath is a generalized pattern to access a specific value. During runtime, the xpath will be expanded for specific values of the indices
        /// </returns>
        protected void generateModel(String json, String rootXPath, String tableNameXPath, String logMessages) throws Exception
        {
            JsonObject jp = new JsonObject(json);

            StringBuilder logs = new StringBuilder();

            if (jp.get(rootXPath) == null || jp.get(rootXPath).getValueType() != JsonObject.XPathTokenType.KEY)
            {
                logs.append("Root property must contain an Object. Null, value, or list found.\n");
            }
            else if (jp.get(tableNameXPath) == null || jp.get(tableNameXPath).equals(""))
            {
                logs.append("Table Name XPath must point to a valid value within the document.\n");
            }
            else
            {
                modelObject(jp.get(rootXPath), new ArrayList<List<String>>(), 0, jp.getString(tableNameXPath), "");
            }

            logMessages = logs.toString();
        }

        /// <summary>
        /// Generates a model for an Object (viewed as a Dictionary)
        /// </summary>
        /// <param name="jp">JsonObject holding the JSON Document</param>
        /// <param name="ansestry">Holds a list of indexing columns using when processing nested arrays</param>
        /// <param name="nestedTableIdx">The index sequential number</param>
        /// <param name="table">Specific table name to be used in the model. If empty, the property name of the JsonObject will be used.</param>
        /// <param name="columnPrefix">Prefix to use when naming columns (using in nesting)</param>
        protected void modelObject(JsonObject jp, List<List<String>> ansestry, int nestedTableIdx, String table, String columnPrefix) throws Exception
        {
            String tableName = table.equals("")
                ? jp.name
                : table;

            for (int i = 0; jp.getObject("").length() > 0 && i < jp.getObject("").names().length(); i++)
            {
            	String prop = jp.getObject("").names().get(i).toString();
            	
                JsonObject j = jp.get(prop);

                String columnName = columnPrefix.equals("")
                    ? j.name
                    : columnPrefix + "_" + j.name;

                switch (j.getValueType())
                {
                    case VALUE:

                        createTableIfMissing(tableName, ansestry);
                        this.tables.get(tableName).addRow(columnName, inferDataType(j.data).getName(), j.path, getAbbreviatedName(columnName, 30));

                        break;

                    case KEY:
                        modelObject(j, ansestry, nestedTableIdx, table, columnName);
                        break;

                    case INDEX:
                    	List<String> ans = new ArrayList<String>();
                    	ans.add( tableName + "_" + jp.name + "_index");
                    	ans.add( "#" + nestedTableIdx);
                    	ansestry.add(ans);
                        nestedTableIdx++;

                        modelList(j, ansestry, nestedTableIdx, tableName + "_" + jp.name + "_" + j.name, "");

                        ansestry.remove(ansestry.size() - 1);
                        break;
                }
            }
        }

        /// <summary>
        /// Generates a model for a list (viewed as a List)
        /// </summary>
        /// <param name="jp">JsonObject holding the JSON Document</param>
        /// <param name="ansestry">Holds a list of indexing columns using when processing nested arrays</param>
        /// <param name="nestedTableIdx">The index sequential number</param>
        /// <param name="table">Specific table name to be used in the model. If empty, the property name of the JsonObject will be used.</param>
        /// <param name="columnPrefix">Prefix to use when naming columns (using in nesting)</param>
        protected void modelList(JsonObject jp, List<List<String>> ansestry, int nestedTableIdx, String table, String columnPrefix) throws Exception
        {
            String tableName = table.equals("")
                ? jp.name
                : table;

            int index = 0;
            for (int i = 0; i < jp.getArray("").length();)
            { 
            	Object row = jp.getArray("").get(i);
            	
                // Create a brand new fork of the current JsonObject to process a child table
                JsonObject j = new JsonObject("{}");
                j.data = row;
                j.index = index;
                j.name = "";
                j.path = jp.path + "[" + index + "]";
                j.root = jp.root;

                switch (j.getValueType())
                {
                    case VALUE:

                        String columnName = columnPrefix.equals("")
                            ? j.name
                            : columnPrefix + "_" + j.name;

                        createTableIfMissing(tableName, ansestry);

                        this.tables.get(tableName).addRow(columnName, inferDataType(j.data).getName(), j.path, getAbbreviatedName(columnName, 30));

                        break;

                    case KEY:

                        modelObject(j, ansestry, nestedTableIdx, table, j.name);

                        break;

                    case INDEX:

                        throw new Exception("Array elements cannot be arrays as we cannot infer the table structure from anonymous values.");
                }

                return;
            }
        }

        /// <summary>
        /// A helper method to intitialize a model table
        /// </summary>
        /// <param name="tableName">The name of the table</param>
        /// <param name="ansestry">A set of column definitions that represent the nesting properties of the table</param>
        /// <param name="model">The model to update</param>
        protected void createTableIfMissing(String tableName, List<List<String>> ansestry) throws Exception
        {
            if (!this.tables.containsKey(tableName))
            {
                // Create the table and add default columns
                DataTable table = new DataTable(tableName);
                table.columns.add(new DataColumn("column", String.class));
                table.columns.add(new DataColumn("datatype", String.class));
                table.columns.add(new DataColumn("xpath", String.class));
                table.columns.add(new DataColumn("shortColumn", String.class));

                // Add line items for the reference to the root table
                table.addRow("DOC_ID", String.class.getName(), "#ID", "DOC_ID");

                // Add indexers for each level of nesting based on the ansestry
                for (List<String> row : ansestry)
                {
                    table.addRow(row.get(0), Long.class.getName(), row.get(1), getAbbreviatedName(row.get(0), 30));
                }

                this.tables.put(table.name, table);
            }
        }

        /// <summary>
        /// Infers a datatype from a value
        /// </summary>
        /// <param name="value">The value to analyse</param>
        /// <returns>A Type that corresponds to the value</returns>
        protected Class<?> inferDataType(Object value)
        {
            // Handle strong types
            if (value instanceof Boolean)
                return Boolean.class;

        	if (value instanceof Double)
                return Double.class;

        	if (value instanceof Long || value instanceof Integer)
                return Long.class;
        
            // It should be a String of some sort
        	String valueStr = value.toString();
            
            try {
            	DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZZ").parseDateTime(valueStr);
            	return Date.class;
            }
            catch(IllegalArgumentException pe) {}
            
            try {
            	DateTimeFormat.forPattern("MM/dd/yyyy").parseDateTime(valueStr);
            	return Date.class;
            }
            catch(IllegalArgumentException pe) {}
           
            return String.class;

        }

        /// <summary>
        /// Attempts to abbreviate a String so that it fits a character limit by removing vowels starting from the tail
        /// </summary>
        /// <param name="name">The String to be abbreviated</param>
        /// <param name="charLimit">The character limit</param>
        /// <returns>The abbreviated String</returns>
        protected String getAbbreviatedName(String name, int charLimit)
        {
            // Return if the name is equal or below the char limit
            if (name.length() <= charLimit)
            {
                return name;
            }

            String res = name;
            char[] vowels = new char[] { 'a', 'e', 'o', 'u', 'i' };

            // Incrementatlly remove vowels until you get below or at the char limit
            int idx = lastIndexOfAny(res, vowels);

            while (res.length() > charLimit && idx >= 0)
            {
                idx = lastIndexOfAny(res, vowels);

                if (idx >= 0 && idx < res.length())
                {
                    res = res.substring(0, idx) + res.substring(idx + 1);
                }

            }

            return res;
        }

        
        private int lastIndexOfAny(String str, char[] characters) {
        	
        	int res = -1;
        	
        	for(char c: characters) {
        		res = Math.max(res, str.lastIndexOf(c));
        	}
        	
        	return res;
        	
        }
        

    }
