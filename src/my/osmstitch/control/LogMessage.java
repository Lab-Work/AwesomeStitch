package my.osmstitch.control;

public class LogMessage {
	private long timestamp;
	private String tag;
	private String msg;

	public LogMessage(String tag, String msg){
		this.timestamp = System.currentTimeMillis();
		this.tag = tag;
		this.msg = msg.replace("#", "").replace("|", "");
	}

	public LogMessage(String csvLine){
		String[] tok = csvLine.split("\\|");
		if(tok.length>0)
			this.timestamp = Long.parseLong(tok[0]);
		else
			this.timestamp = -1;
		
		if(tok.length>1)
			this.tag = tok[1];
		else
			this.tag = "<notag>";
		
		if(tok.length>2)
			this.msg = tok[2].replace('#', '\n');
		else
			this.msg = "";
	}

	public String toCSVLine(){
		return this.timestamp + "|" + this.tag + "|" + this.msg.replace('\n', '#');
	}

	public static String makeSize(String str, int size){
		String newStr = str.toString();

		if(newStr.length() > size){
			newStr = newStr.substring(0, size);
			return newStr;
		}

		while(newStr.length() < size){
			newStr += " ";
		}
		return newStr;
	}
	
	
	public static String makeSize(String str, int size, char c){
		String newStr = str.toString();

		if(newStr.length() > size){
			newStr = newStr.substring(0, size);
			return newStr;
		}

		while(newStr.length() < size){
			newStr += c;
		}
		return newStr;
	}
	

	public String toString(){
		String col1 = makeSize(""+timestamp, 15);
		String col2 = makeSize(tag, 15);
		String col3 = msg;
		return col1 + "   " + col2 + "   " + col3;
	}
	
	public static String logHeader(){
		String col1 = makeSize("Timestamp", 15);
		String col2 = makeSize("Tag", 15);
		String col3 = "Message";
		
		String header = col1 + "   " + col2 + "   " + col3 + "\n";
		for(int i = 0; i < 50; i++)
			header += "-";
		header += "\n";
		return header;
	}
	
	
	public boolean matches(String tag){
		return tag.equals(this.tag);
	}

}
