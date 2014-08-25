package my.osmstitch.control;


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

import my.osmstitch.control.DBConnection;



import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.postgresql.geometric.PGline;
import org.postgresql.geometric.PGpoint;

import my.osmstitch.mapobjects.BBox;
import my.osmstitch.mapobjects.ChangeLog;
import my.osmstitch.mapobjects.CountingNode;
import my.osmstitch.mapobjects.DetailLink;
import my.osmstitch.mapobjects.DetailLinkMap;
import my.osmstitch.mapobjects.DetailLinkMapping;
import my.osmstitch.mapobjects.DetailNode;
import my.osmstitch.mapobjects.Link;
import my.osmstitch.mapobjects.Node;
import my.osmstitch.mapobjects.Tile;



public class OSMParser {
	public static long DISTANT_FUTURE = 40000000000000L;








	public static BBox grabBoundingBox(String fileName, double[] coordinates){

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
	 * Helper method - tells whether two nodes have identical properties. This is useful because we don't want to make a new version of a node
	 * just because it has a new OSM_changeset - the new one may be identical to the old.
	 * @param node1 Any node
	 * @param node2 Any other node
	 * @return True if the nodes are identical, False otherwise
	 */
	public static boolean identical(Node node1, Node node2){
		if(node1.getGeom().x != node2.getGeom().x) return false;
		if(node1.getGeom().y != node2.getGeom().y) return false;
		if(node1.getOsm_traffic_controller() != node2.getOsm_traffic_controller()) return false;
		if(node1.getOsm_num_ways() != node2.getOsm_num_ways()) return false;

		return true;
	}

	/**
	 * Helper method - tells whether two links have identical properties.
	 * @param link1 Any Link
	 * @param link2 Any other Link
	 * @return True if the links are identical, False otherwise
	 */
	public static boolean identical(Link link1, Link link2){
		//System.out.println("is " + link1 + " identical to " + link2 + " ?");

		if(link1.getGeom().getPoint(0).x != link2.getGeom().getPoint(0).x) return false;
		if(link1.getGeom().getPoint(0).y != link2.getGeom().getPoint(0).y) return false;
		if(link1.getGeom().getPoint(1).x != link2.getGeom().getPoint(1).x) return false;
		if(link1.getGeom().getPoint(1).y != link2.getGeom().getPoint(1).y) return false;
		//System.out.println("geom identical");
		if(link1 instanceof DetailLink && link2 instanceof DetailLink){

			DetailLink d_link1 = (DetailLink)link1;
			DetailLink d_link2 = (DetailLink)link2;
			if(d_link1.getProc_link()!=null){
				//System.out.println("**1" + d_link1.getProc_link());
				d_link1.setProc_link_id(d_link1.getProc_link().getId());
			}
			if(d_link2.getProc_link()!=null){
				//System.out.println("**2");
				d_link2.setProc_link_id(d_link2.getProc_link().getId());
			}


			//System.out.println("DETAIL LINKS " + d_link1.getProc_link_id() + ", " + d_link2.getProc_link_id());
			if(d_link1.getProc_link_id() != d_link2.getProc_link_id()) return false;
		}
		//System.out.println("reference identical");

		//if(link1.getStreet_length() != link2.getStreet_length()) return false;

		//System.out.println("length identical");

		if(link1.getOsm_name()==null && link2.getOsm_name()==null) return true;
		
		if(link1.getOsm_name()==null || link2.getOsm_name()==null) return true;
		
		if(!link1.getOsm_name().equals(link2.getOsm_name())) return false;
		//System.out.println("name identical");


		//System.out.println("Yes.");
		return true;
	}

	public static void resolveDifferencesInDB(BBox newMap, BBox oldMap, long NOW, String fileName, String description, boolean isProcessed){

		ChangeLog chLog;

		if(isProcessed)
			chLog = new ChangeLog(NOW, fileName, description);
		else
			chLog = new ChangeLog(NOW, fileName, "DETAILED MAP : " + description);


		//Before we start inserting things into the DB, get the correct #inlinks and #outlinks
		for(Node n : newMap.getAllNodes()){
			n.setNum_in_links((short)newMap.getInLinks(n.getId()).size());
			n.setNum_out_links((short)newMap.getOutLinks(n.getId()).size());
		}


		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 3 - Compare the Node sets in these two BBoxes and record any changes in the DB
		List<Node> nodesToUpdate = new LinkedList<Node>(); //A list of Nodes whose death_timestamp we will update
		for(long nodeId : newMap.nodes.keySet()){
			if(!oldMap.safeHasNode(nodeId)){
				//Case (A) - the Node exists in OSM but not in our DB. So we must add it.
				Node node = newMap.getNode(nodeId);
				node.setBirth_timestamp(NOW);				//The node is born now
				node.setDeath_timestamp(DISTANT_FUTURE);	//The node is alive indefinitely
				node.setGenIdOnInsert(false);				//The node ID is imported from OSM, so we need not generate a new one
				DBConnection.insertLater(node);
				//No other changes need to be made since there is no previous version of this node to update
				chLog.incrementNodes_added();
			}
			else{
				Node newNode = newMap.getNode(nodeId);
				Node oldNode = oldMap.getNode(nodeId);
				if(newNode.getOsm_changeset() > oldNode.getOsm_changeset() && !identical(newNode, oldNode)){
					//Case (B) - the Node exists in both OSM and our DB, and the OSM version is newer
					//So we must add the new one and mark the death_timestamp on the old one
					newNode.setBirth_timestamp(NOW);			//The new node is born now
					newNode.setDeath_timestamp(DISTANT_FUTURE);	//The new node is alive indefinitely
					newNode.setGenIdOnInsert(false);			//The node ID is imported from OSM, so we need not generate a new one
					DBConnection.insertLater(newNode);

					oldNode.setDeath_timestamp(NOW);				//Deprecating the old Node
					nodesToUpdate.add(oldNode);					//We will have to update this Node's death_timestamp in the DB
					chLog.incrementNodes_updated();
				}
				else{
					//CASE (C) - the Node exists in both OSM and our DB, but the DB is up-to-date
					//So do nothing
					chLog.incrementNodes_untouched();
				}
			}

		}

		for(long nodeId : oldMap.nodes.keySet()){
			Node node = oldMap.nodes.get(nodeId);
			if(!newMap.safeHasNode(nodeId) && newMap.getMaxChangeSet() > node.getOsm_changeset()){
				//CASE (D) - the Node exists in our DB, but not in OSM, so we must mark it as dead
				//Note that we also check that the maximum changeset in the osm box is greater than this node's changeset
				//This prevents us from marking a node as dead just because we read an old OSM file
				node.setDeath_timestamp(NOW);
				nodesToUpdate.add(node);

				chLog.incrementNodes_deleted();
			}
		}

		//Update the the nodes who have changed death_timestamps
		DBConnection.updateNodeDeathTimestamps(nodesToUpdate, isProcessed);
		//Flush Nodes buffer to DB so new Links can reference them
		DBConnection.flush(new Node().getTableName());
		DBConnection.flush(new DetailNode().getTableName());

		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 4 - Compare the Link sets in the two BBoxes and record any changes in the DB
		List<Link> linksToUpdate = new LinkedList<Link>();	//A list of Links whose death_timestamps we will update
		List<CountingNode> countingNodesToUpdate = new LinkedList<CountingNode>();
		for(List<Long> nodeIds : newMap.links.keySet()){
			long begin_node_id = nodeIds.get(0);
			long end_node_id = nodeIds.get(1);

			if(!oldMap.safeHasLink(begin_node_id, end_node_id)){
				//Case (A) - the Node exists in OSM but not in our DB. So we must add it.
				Link link = newMap.getLink(begin_node_id, end_node_id);
				link.setBirth_timestamp(NOW);				//the Link is born now
				link.setDeath_timestamp(DISTANT_FUTURE);	//the Link is alive indefinitely
				DBConnection.insertLater(link);

				chLog.incrementLinks_added();
			}
			else{
				Link osmLink = newMap.getLink(begin_node_id, end_node_id);
				Link dbLink = oldMap.getLink(begin_node_id, end_node_id);

				if(osmLink.getOsm_changeset() >= dbLink.getOsm_changeset() && !identical(osmLink, dbLink)){

					//Case (B) - the Link exists in both OSM and our DB, and the OSM version is newer
					//So we must add the new version and mark the death_timestamp on the old version
					osmLink.setBirth_timestamp(NOW);			//The Link is born now
					osmLink.setDeath_timestamp(DISTANT_FUTURE);	//The Link is alive indefinitely
					osmLink.setId(dbLink.getId());				//Copy the ID from the old Link
					osmLink.setGenIdOnInsert(false);			//This ID should not be overwritten when it is added to the DB
					DBConnection.insertLater(osmLink);

					dbLink.setDeath_timestamp(NOW);				//The old link dies now
					for(long id : oldMap.countingNodes.keySet()){
						CountingNode cn = oldMap.countingNodes.get(id);
						if(cn.getLink1_id()==dbLink.getId() || cn.getLink2_id()==dbLink.getId()){
							cn.setDeath_timestamp(NOW);
							cn.setName_filter(dbLink.getOsm_name());
							if(!countingNodesToUpdate.contains(cn))
								countingNodesToUpdate.add(cn);
						}
					}
					linksToUpdate.add(dbLink);					//This death_timestamp must be reflected in the DB

					chLog.incrementLinks_updated();
				}
				else{
					//Case (C) - the Node exists in both OSM and our DB, but the DB is up-to-date
					//So do nothing.
					osmLink.setId(dbLink.getId());
					chLog.incrementLinks_untouched();
				}
			}


		}

		for(List<Long> nodeIds : oldMap.links.keySet()){
			long begin_node_id = nodeIds.get(0);
			long end_node_id = nodeIds.get(1);
			Link link = oldMap.getLink(begin_node_id, end_node_id);

			if(!newMap.safeHasLink(begin_node_id, end_node_id) && newMap.getMaxChangeSet() > link.getOsm_changeset()){
				//Case (D) - the Link exists in our DB but not in OSM, so we must mark it as dead
				//Note that we also check that the maximum changeset in the osm box is greater than this link's changeset
				//This prevents us from marking a link as dead just because we read an old OSM file
				link.setDeath_timestamp(NOW);	//The link dies now
				linksToUpdate.add(link);		//This change must be reflected in the DB

				chLog.incrementLinks_deleted();
			}

		}

		//Update the the links who have changed death_timestamps
		DBConnection.updateLinkDeathTimestamps(linksToUpdate, isProcessed);

		//Flush Links buffer to DB
		DBConnection.flush(new Link().getTableName());
		DBConnection.flush(new DetailLink().getTableName());

		//Update the link counts on our nodes
		List<Node>  borderNodes = new LinkedList<Node>();
		for(Node n : newMap.getAllNodes()){
			if(!newMap.inBox(n))
				borderNodes.add(n);
		}

		DBConnection.updateNodeLinkCounts(borderNodes, NOW, isProcessed);






		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 5 - Compare the DetailLinkMapping sets in the two BBoxes and record any changes in the DB
		if(newMap.getDetailLinkMappings()!=null){

			//Some links in the new map may not have proper IDs - this occurs if they didn't need to be inserted
			for(Link link : newMap.getAllLinks()){
				if(link.getId()==0){
					long newId = DBConnection.lookupLinkId(link, NOW);
					link.setId(newId);
				}
			}


			System.out.println("changing detail_link_mappings");
			List<DetailLinkMapping> dlmToUpdate = new LinkedList<DetailLinkMapping>();	//A list of DetailLInkMappings whose death_timestamps we will update
			for(DetailLinkMapping dlm : newMap.getDetailLinkMappings()){

				Link link = dlm.getProcessed();


				if(newMap.hasLink(link.getBegin_node_id(), link.getEnd_node_id()) && !oldMap.hasDLM(dlm.getLink_id(), dlm.getDetail_link_id())){
					//Case (A) - the DLM exists in OSM but not in our DB. So we must add it.
					dlm.setBirth_timestamp(NOW);				//the Link is born now
					dlm.setDeath_timestamp(DISTANT_FUTURE);	//the Link is alive indefinitely
					DBConnection.insertLater(dlm);
				}
				else{

					//Case B - the DLM exists in both the new and old maps
					//We don't need to do anything
				}


			}

			for(DetailLinkMapping dlm : oldMap.getDetailLinkMappings()){

				Link link = oldMap.getLinkById(dlm.getLink_id());

				if(link !=null && oldMap.hasLink(link.getBegin_node_id(), link.getEnd_node_id()) && !newMap.hasDLM(dlm.getLink_id(), dlm.getDetail_link_id())){
					//Case (D) - the DLM exists in our DB but not in OSM, so we must mark it as dead
					//Note that we also check that the maximum changeset in the osm box is greater than this link's changeset
					//This prevents us from marking a link as dead just because we read an old OSM file
					dlm.setDeath_timestamp(NOW);	//The DLM dies now
					dlmToUpdate.add(dlm);		//This change must be reflected in the DB

				}

			}

			//Update the the links who have changed death_timestamps
			DBConnection.updateDLMDeathTimestamps(dlmToUpdate);

			//Flush Mappings buffer to DB
			DBConnection.flush(new DetailLinkMapping().getTableName());
		}
		else
			System.out.println("not changing detail_link_mappings");

		//Update death_timestamps on CountingNodes whose underlying links have been modified
		DBConnection.updateCountingNodeDeathTimestamps(countingNodesToUpdate);
		for(CountingNode cn : countingNodesToUpdate){
			//Attempt to add a new one at the same place.
			DBConnection.createCountingNode(cn.getGeom().x, cn.getGeom().y, cn.getName(), cn.getName_filter());
		}


		//Record this change in the change log
		DBConnection.insertNow(chLog);

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
		BBox newDetailedMap = OSMParser.grabBoundingBox(fileName, coordinates);

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


		//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
		//Step 3 - Make necessary changes to preprocessed (concise) map in DB
		//A) Get the newest version of the detailed map (note that this may not be equal to the old osMap or dbMapDetailed due to changes and safety margins)
		oldDetailedMap = DBConnection.boundingBoxQuery(newDetailedMap.leftLon, newDetailedMap.topLat, newDetailedMap.rightLon, newDetailedMap.bottomLat, NOW, false, true, true, false);
		System.out.println("Loaded old detailed map.");
		System.out.println(oldDetailedMap);
		BBox newProcessedMap = oldDetailedMap.preprocess();
		System.out.println("Processed it.");
		System.out.println(newProcessedMap);
		//B) Load the old version of the preprocessed map from the DB (newest version BEFORE the current)
		BBox oldProcessedMap = DBConnection.boundingBoxQuery(newDetailedMap.leftLon, newDetailedMap.topLat, newDetailedMap.rightLon, newDetailedMap.bottomLat,NOW, true, true, true, true);
		System.out.println("Loaded old processed map.");
		System.out.println(oldProcessedMap);

		System.out.println("Resolving differences.");
		//C) Compare the old processed map against the new one that we created via preprocessing
		resolveDifferencesInDB(newProcessedMap, oldProcessedMap, NOW, fileName, description, true);



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
