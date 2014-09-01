package my.awesomestitch.control;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import my.awesomestitch.control.DBConnection;




import my.awesomestitch.mapobjects.BBox;
import my.awesomestitch.mapobjects.ChangeLog;
import my.awesomestitch.mapobjects.CountingNode;
import my.awesomestitch.mapobjects.DetailLink;
import my.awesomestitch.mapobjects.DetailLinkMap;
import my.awesomestitch.mapobjects.DetailLinkMapping;
import my.awesomestitch.mapobjects.DetailNode;
import my.awesomestitch.mapobjects.Link;
import my.awesomestitch.mapobjects.Node;
import my.awesomestitch.mapobjects.Tile;
import my.awesomestitch.mapobjects.Way;
import my.awesomestitch.mapobjects.XMLTag;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGpoint;



public class ParserThread extends Thread{

	public static BBox parseBBox(String fileName, double[] coordinates){

		double left = 0, top = 0, right = 0, bottom = 0;
		if(coordinates!=null){
			left = coordinates[0];
			top = coordinates[1];
			right = coordinates[2];
			bottom = coordinates[3];
		}


		//list of ways
		ArrayList<Way> way_list = new ArrayList<Way>();

		//the way that we are currently building, node by  node
		Way currentWay = null;

		long start_time = System.currentTimeMillis();
		Log.v("OSM", "Started parsing " + fileName);


		BBox tmp_bbox = new BBox(left, top, right, bottom); 		//A box that will temporarily hold nodes as we read the file



		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//STEP 1 : Load all nodes from the file
		//STEP 2 : Load all ways from the file (and update the way-count for referenced nodes)
		//these steps are done together
		List<XMLTag> tags;
		try {
			tags = XMLTag.parseFile(fileName);
		} catch (FileNotFoundException e) {
			Log.v("OSM", "Incorrect file name: " + fileName);
			Log.e(e);
			return null;
		}

		for(XMLTag tag : tags){
			//System.out.println(line);
			//parse the line into an XML tag and check the type
			try{
				if(tag.getType().equals("bounds") && coordinates==null){
					tmp_bbox.leftLon = Double.parseDouble(tag.getValue("minlon"));
					tmp_bbox.rightLon = Double.parseDouble(tag.getValue("maxlon"));
					tmp_bbox.bottomLat = Double.parseDouble(tag.getValue("minlat"));
					tmp_bbox.topLat = Double.parseDouble(tag.getValue("maxlat"));

				}
				if(tag.getType().equals("node")){
					//if it's a node, put it into the node table
					Node n = new Node(tag);
					//long osmId = Long.parseLong(tag.getValue("id"));
					//osmNodeLookup.put(new Long(osmId), n);
					//n.insertIntoDB();
					tmp_bbox.add(n);
				}
				else if(tag.getType().equals("way")){
					//if it's a way, extract the way id
					currentWay = new Way();
					currentWay.id = Long.parseLong(tag.getValue("id"));
					currentWay.changeset = Long.parseLong(tag.getValue("changeset"));
				}
				else if(tag.getType().equals("nd")){
					//if it's an nd tag, add this node to the current Way that we are building
					//Node n = osmNodeLookup.get(new Long(osmId));
					long osmId = Long.parseLong(tag.getValue("ref"));
					Node n = tmp_bbox.getNode(osmId);
					currentWay.nodes.add(n);
				}
				else if(tag.getType().equals("tag")&& currentWay != null){
					//if it's a way tag, we may be able to extract the street name or class
					String key = tag.getValue("k");
					if(key.equals("name"))
						currentWay.name = tag.getValue("v"); //get streetname from the tag
					if(key.equals("ref") && currentWay.name==null)
						currentWay.name = tag.getValue("v");
					else if(key.equals("highway"))
						currentWay.street_class = tag.getValue("v"); //get street class
					else if(key.equals("oneway")){ //get one-way street status
						String val = tag.getValue("v");
						if(val.equals("yes") || val.equals("true") || val.equals("1")) //one way streets
							currentWay.oneWay = true;
						if(val.equals("no") || val.equals("false") || val.equals("0")) //normal streets
							currentWay.oneWay = false;
						if(val.equals("reverse") || val.equals("-1")){  	//one way streets that are encoded backwards
							currentWay.oneWay = true;
							currentWay.backwards = true;
						}
					}

				}
				else if(tag.getType().equals("/way")){
					//done building the current way
					//update the nodes within this way and add the way to the list
					if(currentWay.isImportant()){
						currentWay.updateNodes();
						way_list.add(currentWay);
					}


				}
			}
			catch(Exception e){
				Log.e(e);
				Log.v("ERROR", "Ignoring...");
			}
		}


		Log.v("OSM", "tmp_bbox : " + tmp_bbox);

		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 3 : Determine the true size of the boundingbox.  This could be extracted from the OSM data, or it could be
		//given to us as arguments (left, top, right, bottom)
		double new_left, new_top, new_right, new_bottom;
		if(coordinates==null){
			new_left = tmp_bbox.leftLon;
			new_top = tmp_bbox.topLat;
			new_right = tmp_bbox.rightLon;
			new_bottom = tmp_bbox.bottomLat;
		}
		else{
			new_left = left;
			new_top = top;
			new_right = right;
			new_bottom = bottom;
		}

		//System.out.println("New: " + new_left + "," + new_top + "," + new_right + "," + new_bottom);

		//This BBox will contain ALL of the nodes and links from this region of the OSM file.
		//This amount of detai is useful for visualization.
		BBox detail_bbox = new BBox(new_left, new_top, new_right, new_bottom);


		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 4 : we don't care about intermediate nodes along a path
		//only intersections (those with more than one way passing through) and endpoints are relevant
		//so add all of these nodes to the important_bbox
		for(Node n : tmp_bbox.getAllNodes()){
			//Add to the detail_bbox as long as there is at least one Way going through it
			if(n.getOsm_num_ways() > 0)
				detail_bbox.add(new DetailNode(n));
		}

		//DBConnection.flush((new Node()).getTableName());


		Log.v("OSM", "Processing " + way_list.size() + " ways.");


		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 5 : create links between these relevant nodes and add them to the important_bbox
		for(Way way : way_list){
			//start with the first node in the Way
			Node prevProcessedNode = way.nodes.get(0);
			int prevProcessedId = 0;


			//We will use this to keep track of DetailLinks that, together, form a Link
			//For example, a curved road between two intersections will be one Link, but many DetailedLinks
			//We keep separate sets for forward and backward links
			Set<DetailLink> forwardDetailedLinkGroup = new HashSet<DetailLink>();
			Set<DetailLink> backwardDetailedLinkGroup = new HashSet<DetailLink>();



			//We also want to store the length of these links - for our processed links, we cannot simply calculate the length
			//instead, we must add the lengths of its constituent parts
			double forwardProcessedLength = 0;
			double backwardProcessedLength = 0;

			float stored_start_angle=-1;
			float stored_end_angle = -1;

			//if this node is relevant, link it to the previous relevant node.
			int x = 0;
			for(int i = 0; i < way.nodes.size(); i++){
				Node thisNode = way.nodes.get(i);

				///System.out.println(n + " ) " + n.isWayEnd() +"," + n.getOsm_num_ways() );


				//Add links into the detailed_bbox
				if(i>0 && way.nodes.get(i) != null && way.nodes.get(i-1)!=null){
					Node oldNode = way.nodes.get(i-1);
					Node newNode = way.nodes.get(i);
					if(!way.oneWay || !way.backwards){
						DetailLink forwardLink = new DetailLink(oldNode, newNode, way.name, way.street_class, way.id);
						forwardLink.setOsm_changeset(way.changeset);
						forwardLink.setStreet_length(forwardLink.haversine());
						forwardProcessedLength += forwardLink.getStreet_length();
						//if we didn't just insert this forward link, insert it now
						if(!forwardDetailedLinkGroup.contains(forwardLink)){
							forwardDetailedLinkGroup.add(forwardLink);
							//DBConnection.insertLater(forwardLink);
							detail_bbox.add(forwardLink);
						}


					}
					if(!way.oneWay || way.backwards){
						DetailLink backwardLink = new DetailLink(newNode, oldNode, way.name, way.street_class, way.id);
						backwardLink.setOsm_changeset(way.changeset);
						backwardLink.setStreet_length(backwardLink.haversine());
						backwardProcessedLength += backwardLink.getStreet_length();
						//if we didn't just insert this backward link, insert it now
						if(!backwardDetailedLinkGroup.contains(backwardLink)){
							backwardDetailedLinkGroup.add(backwardLink);
							//DBConnection.insertLater(backwardLink);
							detail_bbox.add(backwardLink);
						}


					}


				}
			}

		}




		long stop_time = System.currentTimeMillis();
		long secs = ((stop_time-start_time) / 1000);

		Log.v("OSM", "Finished parsing " + fileName + " after " + secs + " seconds.");
		Log.v("OSM", detail_bbox + "");

		return detail_bbox;
	}










	/**
	 * Pulls some data within a given BoundingBox out of an OSM file and inserts it into the database.
	 * This is done by first querying the database for existing data in this region, then parsing
	 * an OSM file.  We compare these two sets of data, and determine what changes need to be made to
	 * the DB.  Often, this will involve changing versioning information.
	 * @param fileName The name of the OSM file to be read
	 * @param left The longitude of the left edge of the box
	 * @param top The latitude of the top edge of the box
	 * @param right The longitude of the right edge of the box
	 * @param bottom The latitude of the bottom edge of the box
	 */
	public static long insertBoundingBox(String fileName, double[] coordinates, String description){
		//get the current time, which we will use to modify timestamps
		long NOW = System.currentTimeMillis();





		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 1 - read the OSM file.
		BBox newDetailedMap = ParserThread.parseBBox(fileName, coordinates);

		///Plotter.drawMap(osMapDetailed, "pics/osmap_" + System.currentTimeMillis() + ".csv", Plotter.L_NONE, Plotter.N_NONE);



		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 2 - Make necessary changes to detailed map in DB

		//A) query the database for the current version of the map within the region spanned by the OSM file
		long start_time = System.currentTimeMillis();
		BBox oldDetailedMap = DBConnection.boundingBoxQuery(newDetailedMap.leftLon, newDetailedMap.topLat, newDetailedMap.rightLon, newDetailedMap.bottomLat, NOW, false, true, true, false);

		//B) Compare our current DB version of the map against the new version loaded from the OSM file
		//TODO: Make sure references from detailedLink -> processedLink are properly updated
		resolveDifferencesInDB(newDetailedMap,oldDetailedMap, NOW, fileName, description, false);

		oldDetailedMap = null;


		



		//Update the latest map update
		long update_time = DBConnection.getLatestUpdateTime();
		if(NOW > update_time)
			DBConnection.setLatestUpdateTime(NOW);



		long stop_time = System.currentTimeMillis();

		long secs = (stop_time - start_time) / 1000;
		Log.v("OSM", "Finished updating DB after " + secs + " seconds.");





		return NOW;

	}




	public static void main(String[] args){
	
		try {
			DBConnection.initialize("nyc_server_config");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println("file not found.");
		}
		DBConnection.chooseSchema("nyc_map");

		long begin_time = System.currentTimeMillis();
		
		System.out.println("Getting map from DB.");
		BBox detail_box = DBConnection.boundingBoxQuery(-90, 50, -70, 30, begin_time, false, false, true, false);
		System.out.println(detail_box + "");
		
		BBox empty_box = DBConnection.boundingBoxQuery(-90, 50, -70, 30, begin_time, true, false, true, false);

		
		
		System.out.println("Processing map.");
		BBox processed_box = detail_box.preprocess();
		
		System.out.println(processed_box + "");

		System.out.println("Resolving differences in DB.");
		resolveDifferencesInDB(processed_box, empty_box, begin_time, "blah", "blah", true);


		System.out.println("Done.");
		
		


	}







}
