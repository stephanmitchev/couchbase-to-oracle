import java.util.*;

public class DataRow {

	public List<Object> values = new ArrayList<Object>();
	

	public DataRow(Object... objects) {
		
		for(Object o: objects) {
			
			values.add(o);
		}
	}

	public String toString() {
		
		String result = "";
		
		for(Object o: values) {
			
			result += o.toString() + ",";
		}
		
		return result;
	}

}
