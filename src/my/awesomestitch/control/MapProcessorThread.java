package my.awesomestitch.control;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;

import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import my.awesomestitch.control.DBConnection;
import my.awesomestitch.mapobjects.BBox;
import my.awesomestitch.mapobjects.Tile;
import my.awesomestitch.mapobjects.User;
import my.awesomestitch.mapobjects.UserTile;


public class MapProcessorThread extends Thread {
	private static Queue<Tile> toProcessQueue = new LinkedList<Tile>();

	public static List<Tile> processingOrDownloading = new LinkedList<Tile>();

	private static List<MapProcessorThread> runningThreads = new LinkedList<MapProcessorThread>();

	String name;
	static int thread_num = 1;
	Tile tileToProcess = null;






	public MapProcessorThread(Tile tile){
		this.name = "PC " + thread_num++;
	}

	public void echo(String str){
		Log.v("TILE", name + " : " + str);
	}



	@Override
	public void run(){


		echo("got " + tileToProcess);

		//If we were able to get a tile, it must be downloaded and marked for processing
		if(tileToProcess!=null){
			//Get the coordinates of this tile and the current time
			double left = tileToProcess.getLeft_lon();
			double top = tileToProcess.getBottom_lat() + Tile.BIG_TILE_SIZE;
			double right = tileToProcess.getLeft_lon() + Tile.BIG_TILE_SIZE;
			double bottom = tileToProcess.getBottom_lat();
			long NOW =System.currentTimeMillis();

			//Load the newest version of the detailed map from the DB
			BBox newDetailedMap = DBConnection.boundingBoxQuery(left, top, right, bottom, NOW, false, true, true, true);

			//Perform the preprocessing - this is a fairly heavy computation
			BBox newProcessedMap = newDetailedMap.preprocess();

			//Add the processed tile to the list of DB updates
			synchronized(Controller.dbTileLock){
				DBResolverThread.enqueueProcessedBBox(newProcessedMap);
			}



		}
	}

	/*
	public void SendNotificationIfNecessary(Tile justFinished){
		//First, get all UserTiles for this Tile - this will tell us which users are waiting for this Tile.
		List<UserTile> relevantUserTiles = DBConnection.lookupUserTiles(justFinished.getGrid_x(), justFinished.getGrid_y());

		//Get unique set of user IDs
		Set<Long> userSet = new HashSet<Long>();
		for(UserTile ut : relevantUserTiles){
			userSet.add(ut.getUser_id());
		}

		//Now, for each user, determine if they are done waiting, or if they still have other Tiles in the queue
		for(Long user_id : userSet){
			//This user will have a UserTile for every Tile it has ever downloaded or ordered
			List<UserTile> current_user_usertiles = DBConnection.lookupUserTiles(user_id);
			//Use these to get the relevant Tiles
			List<Tile> current_user_tiles = DBConnection.getUniqueTiles(current_user_usertiles);


			//If ALL of these tiles are done downloading, then this user's request is complete
			boolean allDone = true;
			long latestTimestamp = 0;
			Tile newestTile = null;
			for(Tile t : current_user_tiles){
				//If any of these tiles is still downloading, that user is not done
				if(t.isStill_downloading())
					allDone = false;

				if(t.getUpdated_timestamp() > latestTimestamp){
					latestTimestamp = t.getUpdated_timestamp();
					newestTile = t;
				}

			}

			if(allDone){
				//We know when this latest tile was completed - determine when the user ordered it
				//by finding the matching UserTile
				UserTile match = null;
				for(UserTile ut : current_user_usertiles){
					if(ut.getGrid_x()==newestTile.getGrid_x() && ut.getGrid_y()==newestTile.getGrid_y()){
						match = ut;
						break;
					}
				}

				//Prepare Message
				String startTime = DateFormat.getDateInstance().format(new Date(match.getOrdered_timestamp()));
				String endTime = DateFormat.getDateInstance().format(new Date(newestTile.getUpdated_timestamp()));

				String elapsed = timeString(newestTile.getUpdated_timestamp() - match.getOrdered_timestamp());

				String subject = "Your map is ready.";
				String msg = "Hello,\n"
							+ "Recently, you requested to use the TrafficTurk app in an unknown area.  We have "
							+ "prepared the map in this area, and TrafficTurk should now be usable.  Thanks for your patience!\n\n"
							+ "Map requested at       : " + startTime + "\n"
							+ "Map became available at: " + endTime + "\n"
							+ "Total processing time  : " + elapsed + "\n\n"
							+ "Have fun counting,\n"
							+ "TrafficTurk";

				//Get username (email address)
				User usr = DBConnection.lookupUser(match.getUser_id());

				String emailAddress = usr.getUsername();

				Log.v("TILE", "Sending notification email to " + emailAddress);

				sendNotificationEmail(emailAddress, subject, msg);
			}

		}



	}

	public static void joinAll(){

		MapDownloaderThread.joinAll();

		List<MapProcessorThread> threads = new LinkedList<MapProcessorThread>(runningThreads);

		for(MapProcessorThread thread : threads){
			try {
				thread.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				Log.e(e);
			}
		}
	}


	public static String timeString(long millis){
		long total = (millis / 1000);

		int sec = (int) (total % 60);
		total /= 60;
		int min = (int)(total % 60);
		total /=60;
		int hour = (int)(total % 60);


		return hour + " Hours, " + min + " Minutes, " + sec + " Seconds";

	}


	public static void sendNotificationEmail(String to, String subject, String body){
		String from = "trafficturknotifier@gmail.com";
		String pass = "tr@fficturk2013";

		//Setup properties for SMTP server
		Properties props = System.getProperties();
		String host = "smtp.gmail.com";
		props.put("mail.smtp.starttls.enable", "true");
		props.put("mail.smtp.host", host);
		props.put("mail.smtp.user", from);
		props.put("mail.smtp.password", pass);
		props.put("mail.smtp.port", "587");
		props.put("mail.smtp.auth", "true");



		Session session = Session.getDefaultInstance(props);
		MimeMessage message = new MimeMessage(session);

		try {
			message.setFrom(new InternetAddress(from));
			InternetAddress toAddress = new InternetAddress(to);
			message.addRecipient(Message.RecipientType.TO,toAddress);

			message.setSubject(subject);
			message.setText(body);
			Transport transport = session.getTransport("smtp");
			transport.connect(host, from, pass);
			transport.sendMessage(message, message.getAllRecipients());
			transport.close();
		}
		catch (AddressException ae) {
			ae.printStackTrace();
		}
		catch (MessagingException me) {
			me.printStackTrace();
		}
	}
	 */
}
