
/**
 * @author smitchev
 *
 */
public class DataColumn {
	
	public String name;
	public Class<?> dataType;
	
	public DataColumn(String columnName, Class<?> type) {
		name = columnName;
		dataType = type;
		
	}
	
	public DataColumn clone() {
		return new DataColumn(name, dataType);
	}

	public String toString() {
	
		return name + " of type " + dataType.getName();
	
	}
}
