package my.awesomestitch.control;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.LinkedList;
import java.util.List;

public class Log {
	private static String fileName = "server.log";
	private static File f;
	private static PrintWriter writer;
	private static BufferedReader reader;

	private static int MSG_BUFFER_SIZE = 1000;
	

	
	static LinkedList<LogMessage> logTail;
	
	
	public static void initialize(){
		try{
			f = new File(fileName);
			writer = new PrintWriter(new BufferedWriter(new FileWriter(f, true)));
			reader = new BufferedReader(new FileReader(f));
		}
		catch(IOException e){}
		logTail = new LinkedList<LogMessage>();
		
		String line;
		try {
			while((line=reader.readLine())!=null){
				LogMessage lm = new LogMessage(line);
				addMessage(lm);
			}
		} catch (IOException e) {
			
		}
	}
	
	public static void initialize(String logFileName){
		fileName = logFileName;
		initialize();
	}


	public static void v(String tag, String message){
		if(writer==null)
			initialize();
		LogMessage lMsg = new LogMessage(tag, message);
		writer.println(lMsg.toCSVLine());
		writer.flush();
		addMessage(lMsg);
	}
	
	public static void e(Exception e){
		ByteArrayOutputStream os = new ByteArrayOutputStream();
		PrintStream ps = new PrintStream(os);
		e.printStackTrace(ps);
		String msg = os.toString();
		
		Log.v("ERROR", msg);
	}
	
	
	public static void addMessage(LogMessage lm){
		logTail.addLast(lm);
		if(logTail.size() > MSG_BUFFER_SIZE){
			logTail.removeFirst();
		}
	}
	
	public static List<LogMessage> lookupByTag(String tag){
		LinkedList<LogMessage> matches = new LinkedList<LogMessage>();
		for(LogMessage l : logTail){
			if(l.matches(tag))
				matches.add(l);
		}
		return matches;
	}
	
	public static List<LogMessage> recentMessages(int num){
		if(num > logTail.size() || num==0)
			return logTail;
		else
			return logTail.subList(logTail.size() - num, logTail.size());

	}
	
	
	public static void main(String[] args){
		Log.initialize();
		
		System.out.println("INITIAL");
		List<LogMessage> list = Log.recentMessages(10);
		for(LogMessage lm : list)
			System.out.println(lm);
		System.out.println("\n\n");
		
		List badList = null;
		try{
			badList.add(5);
		}
		catch(Exception e){
			Log.e(e);
		}
		
		
		System.out.println("AFTER");
		list = Log.recentMessages(10);
		for(LogMessage lm : list)
			System.out.println(lm);
		
		
		
	}

}
