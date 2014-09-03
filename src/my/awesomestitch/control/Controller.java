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
			//Find Tiles in the DB where download_status==WAITING
			//These are tiles that are queued for download
			//Then create a new MapDownloaderThread to download the .osm file
			try {
				synchronized(mapDownloaderThreads){
					ResultSet rs = DBConnection.executeQuery("SELECT * FROM tmp_schema.tiles WHERE download_status=" + Tile.WAITING + ";");
					while(rs.next() && numRunningMapDownloaderThreads() < MAX_DOWNLOADER_THREADS){
						Tile tile = new Tile(rs);

						MapDownloaderThread thread = new MapDownloaderThread(tile);
						mapDownloaderThreads.add(thread);

						thread.start();
					}
				}
			} catch (SQLException e) {
				Log.v("DB", "Error searching for tiles queued for download.");
				Log.e(e);
			}


			//If there are any Tiles in the DB where download_status==DONE and detailed_map_status==WAITING
			//These are tiles that are done downloading, and are queued for parsing
			//Then create a new ParserThread to parse the .osm thread
			try{
				synchronized(parserThreads){
					ResultSet rs = DBConnection.executeQuery("SELECT * from tmp_schema.tiles WHERE download_status=" + Tile.DONE+ " AND detailed_map_status=" + Tile.WAITING + ";");

					while(rs.next() && numRunningParserThreads() < MAX_PARSER_THREADS){
						Tile tile = new Tile(rs);

						ParserThread thread = new ParserThread(tile);
						parserThreads.add(thread);
						thread.start();
					}
				}
			} catch(SQLException e){
				Log.v("DB", "Error searching for tiles queued for parsing.");
			}

			//If all tiles in the DB have detailed_map_status==DONE, then the detailed map is fully created
			//It is now possible to begin preprocessing it and creating the processed map
			//all tiles with processed_map_status==WAITING must be processed
			try{
				synchronized(processorThreads){
					//Query for all tiles that are not done with the detailed map parsing
					ResultSet rs = DBConnection.executeQuery("SELECT * from tmp_schema.tiles WHERE detailed_map_status!=" + Tile.DONE + ";");
					//If none exist, then we can proceed
					if(!rs.next()){
						
						ResultSet rs2 = DBConnection.executeQuery("SELECT * from tmp_schema.tiles where processed_map_status=" + Tile.WAITING + ";");

						while(rs.next() && numRunningMapProcessorThreads() < MAX_PROCESSOR_THREADS){
							Tile tile = new Tile(rs);

							MapProcessorThread thread = new MapProcessorThread(tile);
							processorThreads.add(thread);
							thread.start();
						}
					}
				}
			} catch(SQLException e){
				Log.v("DB", "Error searching for tiles queued for preprocessing.");
			}
		}


	}

}
