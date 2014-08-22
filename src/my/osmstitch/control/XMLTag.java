package my.osmstitch.control;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;

/**
 * 
 * @author Brian Donovan <briandonovan100@gmail.com>
 * Parses an XML tag, making it simple to look up attributes. It is intended for use with serial XML files
 * like OSM's planet files because tags can be generated line by line.
 */
public class XMLTag {
	/**
	 * The type of tag, for example a tag <node id=500> would have type "node"
	 */
	private String type;
	/**
	 * A table of parameters within the tag.
	 * For example, the tag <node id=500> would have params.get("id") = 500
	 */
	private Hashtable<String,String> params;
	
	/**
	 * Constructs the tag object from a line of XML text.
	 * @param line the line of XML text that will be parsed
	 */
	public XMLTag(String line){
		//create our table of parameters
		params = new Hashtable<String,String>();
		//split the line into tokens (small substrings which are separated by spaces)
		//such as '<node' or 'id="700"'
		String[] tokens = tokenize(line);
		
		for(String tok : tokens){
			if(tok.contains("<")){
				//first token - extract node type
				int bracketPos = tok.indexOf('<');
				type = tok.substring(bracketPos+1);
			}
			else if(tok.contains("=")){
				//toke contains a parameter - use the positions of = and " to extract the key and value
				int equalsPos = tok.indexOf('=');
				int firstQuotePos = tok.indexOf('\"');
				int secondQuotePos = tok.indexOf('\"', firstQuotePos+1);
				String key = tok.substring(0, equalsPos);
				String value = tok.substring(firstQuotePos+1, secondQuotePos);
				params.put(key, value);
			}
		}
		
	}
	
	/**
	 * Splits the line into an array of Strings wherever spaces exist, ignoring spaces that appear within quotes.
	 * @param line 	the line of text to be parsed
	 * @return		an array of strings, each containing a token
	 */
	private String[] tokenize(String line){
		ArrayList<String> tokens = new ArrayList<String>();
		int tokenStartPos = 0;
		boolean currentlyInQuotes = false;
		
		
		for(int i = 0; i < line.length(); i++){
			if(line.charAt(i)=='\"')
				currentlyInQuotes = !currentlyInQuotes;
			else if((line.charAt(i)==' ' || line.charAt(i)=='>') && !currentlyInQuotes){
				String t = line.substring(tokenStartPos, i);
				tokens.add(t);
				tokenStartPos = i + 1;
			}
			
		}
		//String t = line.substring(tokenStartPos, line.length());
		//tokens.add(t);
		
		
		return tokens.toArray(new String[tokens.size()]);
		
	}
	
	/**
	 * Returns the value of some parameter from the XML tag
	 * @param 	key - the value to lookup.
	 * @return	the value of this parameter
	 */
	public String getValue(String key){
		return params.get(key);
	}
	
	/**
	 * Returns the type of tag (for example node, way, etc...)
	 * @return	the type of tag
	 */
	public String getType(){
		return type;
	}
	
	public static List<XMLTag> parseFile(String fileName) throws FileNotFoundException{
		List<XMLTag> tags = new LinkedList<XMLTag>();
		
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(fileName));
			String line;
			while ((line = br.readLine()) != null) {
				int startPos = 0, endPos = 0;
				for(int i = 0; i < line.length(); i++){
					if(line.charAt(i)=='<')
						startPos = i;
					if(line.charAt(i)=='>'){
						endPos = i + 1;
						String tok = line.substring(startPos,  endPos);
						XMLTag tag = new XMLTag(tok);
						tags.add(tag);
					}
				}
				
			}
			br.close();
		}
		catch(IOException e){
			return tags;
		}
		return tags;
	}

	
	public static void main(String args[]){
		XMLTag tag = new XMLTag("<node id=\"37945137\" lat=\"40.1163740\" lon=\"-88.2702830\" user=\"woodpeck_fixbot\" uid=\"147510\" visible=\"true\" version=\"2\" changeset=\"2730823\" timestamp=\"2009-10-04T01:45:10Z\"/>)");
		Log.v("DB",tag.getType());
		Log.v("DB",tag.getValue("lat"));
		Log.v("DB",tag.getValue("lon"));
	}
}
