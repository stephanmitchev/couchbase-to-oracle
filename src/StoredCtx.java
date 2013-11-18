import java.util.*;

public class StoredCtx
{
	public List<String> ids;
	public int currentIdx;
	public String uniqueName;
	public String tableName;
	public String url;
	public String bucket;
	public String password;
	public String ddoc;
	public String view;
	public JSONDataModel model;
	
	public StoredCtx() 
	{ 
		ids = new ArrayList<String>(); 
		currentIdx = 0;
		uniqueName = "";
		tableName = "";
		url = "";
		bucket = "";
		password = "";
		ddoc = "";
		view = "";
		model = null;
	}
}