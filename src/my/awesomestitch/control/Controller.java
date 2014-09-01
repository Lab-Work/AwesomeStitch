package my.awesomestitch.control;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import my.awesomestitch.mapobjects.Tile;

public class Controller {
	//Keep track of the necessary threads

	private static int MAX_DOWNLOADER_THREADS = 2;
	private static List<MapDownloaderThread> mapDownloaderThreads = new LinkedList<MapDownloaderThread>();

	private static int MAX_PARSER_THREADS = 2;
	private static List<ParserThread> parserThreads = new LinkedList<ParserThread>();

	private static int MAX_PROCESSOR_THREADS = 4;
	private static List<MapProcessorThread> processorThreads = new LinkedList<MapProcessorThread>();

	private static DBResolverThread dbResolverThread = null;
	
	/**
	 * Synchronize on this object when updating tile status.  This prevents DB race conditions.
	 */
	public static final Object dbTileLock = new Object();

	/**
	 * Tells how many MapDownloaderThreads are currently running
	 * @return the number of currently running MapDownloaderThreads
	 */
	private static int numRunningMapDownloaderThreads(){
		int count = 0;
		for(MapDownloaderThread thread : mapDownloaderThreads){
			if(thread.isAlive())
				count++;
		}
		return count;
	}

	/**
	 * Tells how many ParserThreads are currently running
	 * @return the number of currently running ParserThreads
	 */
	private static int numRunningParserThreads(){
		int count = 0;
		for(ParserThread thread : parserThreads){
			if(thread.isAlive())
				count++;
		}
		return count;
	}

	/**
	 * Tells how many MapProcessorThreads are currently running
	 * @return the number of currently running MapProcessorThreads
	 */
	private static int numRunningMapProcessorThreads(){
		int count = 0;
		for(MapProcessorThread thread : processorThreads){
			if(thread.isAlive())
				count++;
		}
		return count;
	}

	/**
	 * Tells whether or not the dbResolverThread is currently running
	 * @return True if there is an instance of DBResolverThread running, False otherwise
	 */
	private static boolean dbResolverIsRunning(){
		if(dbResolverThread==null)
			return false;
		if(dbResolverThread.isAlive())
			return true;
		return false;
	}

	/*
	 	1) Download tile
		2) Parse & Insert detailed map
		3) Preprocess
		4) Insert processed map

		If there are some tiles with download_status=WAITING
		-start MapDownloaderThread (1)

		If there are some tiles with download_status=DONE and detailed_map_status=WAITING
		-start ParserThread (2)

		If all tiles have detailed_map_status=DONE
		-start Preprocessor thread

		If there are any BBoxes in the detailedQueue or processedQueue
		-start DBUpdaterThread
	 */

	public static void startThreadsIfNecessary(){
		synchronized(dbTileLock){
			//If there are any Tiles in the DB where download_status==WAITING
			//Then create a new MapDownloaderThread to handle it
			try {
				synchronized(mapDownloaderThreads){
					ResultSet rs = DBConnection.executeQuery("SELECT * FROM tmp_schema.tiles WHERE download_status=" + Tile.WAITING + ";");
					while(rs.next() && numRunningMapDownloaderThreads() < MAX_DOWNLOADER_THREADS){
						Tile tile = new Tile(rs);
	
						MapDownloaderThread thread = new MapDownloaderThread(tile);
	
						thread.start();
					}
				}
			} catch (SQLException e) {
				Log.v("DB", "Error searching for not-downloaded tiles.");
				Log.e(e);
			}
		}


	}

}
