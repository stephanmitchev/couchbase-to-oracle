import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


    public class JsonObject
    {

        /// <summary>
        /// A type for a token within an XPath expression.
        /// VALUE means that the token is a property that points to a primitive value
        /// KEY  means that the token is a property that points to a dictionary
        /// INDEX means that the token is numeric and points to an object in a list
        /// </summary>
        public enum XPathTokenType
        {
            VALUE,
            KEY,
            INDEX
        }

        /// <summary>
        /// A structure to hold a token
        /// </summary>
        public class XPathToken
        {

            public String name;
            public XPathTokenType type;
        }

        /// <summary>
        /// Raw store for the value of this object
        /// </summary>
        public Object data = null;

        /// <summary>
        /// The root of this object
        /// </summary>
        public JsonObject root = null;
        
        /// <summary>
        /// XPath from the root to this node
        /// </summary>
        public String path = "";

        /// <summary>
        /// Index of this object if found in a list. -1 if this is part of another object
        /// </summary>
        public int index = -1;

        /// <summary>
        /// Name of the property holding this object's value. Empty if this is a part of a list
        /// </summary>
        public String name = "";

        /// <summary>
        /// XPath token cache to speed up the tokenization process
        /// </summary>
        static Map<String, List<XPathToken>> cache = new HashMap<String, List<XPathToken>>();

        /// <summary>
        /// Type of the value that this object represents
        /// </summary>
        public XPathTokenType getValueType() { 
            return data instanceof JSONObject
                ? XPathTokenType.KEY 
                : data instanceof JSONArray
                    ? XPathTokenType.INDEX
                    : XPathTokenType.VALUE;
        }

        /// <summary>
        /// Constructor based on a json document
        /// </summary>
        /// <param name="jsonString">The document that would be processed</param>
        public JsonObject(String jsonString) {

        	JSONTokener jt = new JSONTokener(jsonString);
        	
            data = new JSONObject(jt);
            root = this;
            path = "";
            index = -1;
            
        }

        /// <summary>
        /// Returns a serialized view of the document
        /// </summary>
        /// <returns></returns>
        public String toString()
        {
            return data.toString();
        }

        /// <summary>
        /// Fluent interface over the object to get a value from an XPath
        /// </summary>
        /// <param name="xpath">The XPath to the value</param>
        /// <returns>A JsonObject wrapping the value</returns>
        public JsonObject get(String xpath)
        {

            Object fragment = data;
            List<XPathToken> tokens = tokenize(xpath);
            int index = -1;
            String name = "";
            for (int i = 0; i < tokens.size(); i++)
            {
                if (fragment != null)
                {

                    switch (tokens.get(i).type)
                    {
                        case KEY:
                            fragment = ((JSONObject)fragment).has(tokens.get(i).name) ? ((JSONObject)fragment).get(tokens.get(i).name) : null;
                            index = -1;
                            name = tokens.get(i).name;
                            break;

                        case INDEX:
                            fragment = ((JSONArray)fragment).length() > Integer.parseInt(tokens.get(i).name) ? ((JSONArray)fragment).get(Integer.parseInt(tokens.get(i).name)) : null;
                            index = Integer.parseInt(tokens.get(i).name.replaceAll("[\\[\\]]", ""));
                            break;
                    }
                }
            }

            JsonObject result = new JsonObject("{}");
            result.data = fragment;
            result.root = this.root;
            result.path = (this.path.equals("") ? "" : this.path + "/") + xpath;
            result.index = index;
            result.name = name;

            return result;

        }


        

        /// <summary>
        /// Runs the XPath query and parsed the string representation of the result to int
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>An int-parsed result value from the XPath query</returns>
        public int getInt(String xpath) { return Integer.parseInt(get(xpath).data == null ? "0" : get(xpath).data.toString()); }

        /// <summary>
        /// Runs the XPath query and parsed the string representation of the result to long
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>A long-parsed result value from the XPath query</returns>
        public long getLong(String xpath) { return Long.parseLong(get(xpath).data == null ? "0" : get(xpath).data.toString()); }

        /// <summary>
        /// Runs the XPath query and parsed the string representation of the result to float
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>A float-parsed result value from the XPath query</returns>
        public float getFloat(String xpath) { return Float.parseFloat(get(xpath).data == null ? "0" : get(xpath).data.toString()); }

        /// <summary>
        /// Runs the XPath query and parsed the string representation of the result to double
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>A double(as in class)-parsed result value from the XPath query</returns>
        public double getDouble(String xpath) { return Double.parseDouble(get(xpath).data == null ? "0" : get(xpath).data.toString()); }

        /// <summary>
        /// Runs the XPath query and parsed the string representation of the result to bool
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>A bool-parsed result value from the XPath query</returns>
        public boolean getBool(String xpath) { return Boolean.parseBoolean(get(xpath).data == null ? "false" : get(xpath).data.toString()); }

        /// <summary>
        /// Runs the XPath query and returns the string representation of the result
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>The string representation of the result value from the XPath query</returns>
        public String getString(String xpath)
        {
            JsonObject o = get(xpath);
            return o.data != null ? o.data.toString() : null;
        }

        /// <summary>
        /// Runs the XPath query and returns the Dictionary-casted result
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>The string representation of the result value from the XPath query</returns>
        public JSONObject getObject(String xpath)
        {
            JsonObject o = get(xpath);
            return o.data != null && o.isMap() ? (JSONObject)o.data : new JSONObject();
        }

        public JSONArray getArray(String xpath)
        {
            JsonObject o = get(xpath);
            return o.data != null ? (JSONArray)o.data : new JSONArray();
        }

        /// <summary>
        /// Checks whether the underlying value is an object (Dictionary)
        /// </summary>
        /// <returns>True if the value is a Dictionary; otherwise false</returns>
        public boolean isMap()
        {
            return this.data instanceof JSONObject;
        }
                
        /// <summary>
        /// Checks whether the underlying value is an object (Dictionary)
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>True if the value is a Dictionary; otherwise false</returns>
        public boolean isMap(String xpath)
        {
            return get(xpath).isMap();
        }

        /// <summary>
        /// Checks whether the underlying value is an array (List)
        /// </summary>
        /// <returns>True if the value is a List; otherwise false</returns>
        public boolean isList()
        {
            return this.data instanceof JSONArray;
        }

        /// <summary>
        /// Checks whether the underlying value is an array (List)
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>True if the value is a List; otherwise false</returns>
        public  boolean isList(String xpath)
        {
            return get(xpath).isList();
        }

        /// <summary>
        /// Checks whether a certain XPath query results in a non-null value
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>True if the XPath result is not null; otherwise false</returns>
        public boolean exists(String xpath) { return get(xpath) != null; }
        
        /// <summary>
        /// Returns the count of elements in the case when the result value is a collection in general
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <returns>The count of values within the collection if the value is a collection; 0 otherwise</returns>
        public int count(String xpath)
        {
            Object o = get(xpath);
            if (o.getClass().equals(new HashMap<String, Object>().getClass()))
            {
                return ((JSONObject)o).length();
            }
            else if (o.getClass().equals(new ArrayList<Object>().getClass()))
            {
            	return ((JSONArray)o).length();
            }
            else
                return 0;
        }

        /// <summary>
        /// Adds an item to the object at a given XPath. THe XPath must exist and must be a list
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <param name="data">The value to put at the XPath address</param>
        public void addToList(String xpath, Object data)
        {
            getArray(xpath).put(data);
        }

        /// <summary>
        /// Adds an item to the object at a given XPath. THe XPath must exist and must be a dictionary
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <param name="key">The key at which to put the value</param>
        /// <param name="data">The value to put at the XPath address</param>
        public void putToMap(String xpath, String key, Object data)
        {
            getObject(xpath).put(key, data);
        }

        /// <summary>
        /// Removes an item from the object at a given XPath. The XPath must exist and must be a dictionary
        /// </summary>
        /// <param name="xpath">The XPath query</param>
        /// <param name="key">The key at which to put the value</param>
        public void removeFromMap(String xpath, String key)
        {
            getObject(xpath).remove(key);
        }
 
        /// <summary>
        /// Basic XPath tokenizer with cache support
        /// </summary>
        /// <param name="xpath">The XPath string</param>
        /// <returns>List of tokens extracted from the string</returns>
        List<XPathToken> tokenize(String xpath)
        {

            if (cache.containsKey(xpath))
            {
                return cache.get(xpath);
            }

            Pattern pattern = Pattern.compile("([a-zA-Z:\\-_0-9]+)|(\\[[0-9]+\\])+");
            Matcher matcher = pattern.matcher(xpath);

            List<XPathToken> res = new ArrayList<XPathToken>();

            int index = 0;
            while (matcher.find()) {
            	XPathToken token = new XPathToken();

                if (matcher.group().matches("\\[[0-9]+\\]"))
                {
                    token.type = XPathTokenType.INDEX;
                    token.name = matcher.group().replaceAll("[\\[\\]]", "");
                }
                else if (matcher.group().matches("[a-zA-Z:\\-_0-9]+"))
                {
                    token.type = XPathTokenType.KEY;
                    token.name = matcher.group().replace("/", "");
                }

                res.add(index, token);
                index++;
            }
         
            cache.put(xpath, res);

            return res;
        }
    }
