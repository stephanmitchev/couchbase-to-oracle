import java.util.*;

public class DataTable {
	
	public String name = "";

	public List<DataColumn> columns = new ArrayList<DataColumn>();

	public List<DataRow> rows = new ArrayList<DataRow>();
	
	public DataTable(String tableName) {
		name = tableName;
	}

	public void addRow(Object... o) throws Exception {
		
		if (o.length != columns.size())
			throw new Exception("Row cells do not match the defined columns");
		else {
			
			rows.add(new DataRow(o));
		
		}
		
	}

	public int indexOfColumn(String colName) {
		int result = -1;
		
		for(int i = 0; i < columns.size(); i++) {
			DataColumn col = columns.get(i);
			if (col.name.equals(colName))
				result = i; 
				
		}
		
		return result;
	}
	
	public DataTable clone() {
		DataTable result = new DataTable(name);
		
		for(DataColumn col: columns) {
		
			result.columns.add(col.clone());		
		}
		
		for(DataRow row : rows) {
			Object[] newValues = new Object[row.values.size()];
			
			for(int i = 0; i < row.values.size(); i++)
				newValues[i] = row.values.get(i);
			
			try {
				result.addRow(newValues);
			} catch (Exception e) {
				
			}
		}

		return result;
	}

	public String toString() {
		
		return name + " " + columns.size() + " cols, " + rows.size() + " rows";
	
	}
}
