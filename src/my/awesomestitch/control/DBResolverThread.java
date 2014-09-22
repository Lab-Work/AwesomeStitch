package my.awesomestitch.control;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;

import my.awesomestitch.mapobjects.BBox;
import my.awesomestitch.mapobjects.ChangeLog;
import my.awesomestitch.mapobjects.CountingNode;
import my.awesomestitch.mapobjects.DetailLink;
import my.awesomestitch.mapobjects.DetailLinkMapping;
import my.awesomestitch.mapobjects.DetailNode;
import my.awesomestitch.mapobjects.Link;
import my.awesomestitch.mapobjects.Node;
import my.awesomestitch.mapobjects.Tile;

public class DBResolverThread extends Thread {
	public static final long DISTANT_FUTURE = 40000000000000L;



	public static final int MAX_QUEUE_SIZE=10;
	/**
	 * A Queue of detailed BBoxes which need to be added to the DB
	 */
	private static Queue<BBox> detailedBBoxes = new LinkedBlockingQueue<BBox>(MAX_QUEUE_SIZE);

	/**
	 * A Queue of processed BBoxes which need to be added to the DB
	 */
	private static Queue<BBox> processedBBoxes = new LinkedBlockingQueue<BBox>(MAX_QUEUE_SIZE);

	private boolean isComplete = false;


	/**
	 * Helper method - tells whether two nodes have identical properties. This is useful because we don't want to make a new version of a node
	 * just because it has a new OSM_changeset - the new one may be identical to the old.
	 * @param node1 Any node
	 * @param node2 Any other node
	 * @return True if the nodes are identical, False otherwise
	 */
	private static boolean identical(Node node1, Node node2){
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
	private static boolean identical(Link link1, Link link2){
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

	private static void resolveDifferencesInDB(BBox newMap, BBox oldMap, long NOW, String fileName, String description, boolean isProcessed){

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
	 * Add a detailed BBox into the queue of boxes to be resolved in the DB
	 * And start a DBResolverThread if necessary
	 * @param bbox A detailed BBox
	 */
	public static void enqueueDetailedBBox(BBox bbox){
		detailedBBoxes.add(bbox);
	}

	/**
	 * Add a processed BBox into the queue of boxes to be resolved in the DB
	 * And start a DBResolverThread if necessary
	 * @param bbox A processed BBox
	 */
	public static void enqueueProcessedBBox(BBox bbox){
		processedBBoxes.add(bbox);
	}
	
	/**
	 * Tells whether there are any BBoxes in either queue that need to be processed
	 * @return True if either queue has elements in it, False if both are empty
	 */
	public static boolean hasBBoxesInQueue(){
		return detailedBBoxes.size() > 0 || processedBBoxes.size() > 0;
	}

	public void run(){

		//This thread runs as long as there are still detailedBBoxes or processedBBoxes to add to the DB

		if(detailedBBoxes.size() > 0){
			//Detailed BBoxes are given preference - their queue must be empty before processed BBoxes are even considered
			//This avoids race conditions and also decreases memory usage
			long NOW = System.currentTimeMillis();

			//Get the newly parsed detailedMap from the queue
			BBox newDetailedMap = detailedBBoxes.remove();

			//1) query the database for the current version of the map within the region spanned by the OSM file
			//If this is the first time this tile is added, this may contain a few links that go over the border, etc...
			long start_time = System.currentTimeMillis();
			BBox oldDetailedMap = DBConnection.boundingBoxQuery(newDetailedMap.leftLon, newDetailedMap.topLat, newDetailedMap.rightLon, newDetailedMap.bottomLat, NOW, false, true, true, false);

			String fileName = "";
			String description = "";
			Tile tile = newDetailedMap.getTile();
			if(tile!=null){
				fileName = tile.fileName();
				description = "Update of " + fileName;
			}

			//2) Compare our current DB version of the map against the new version loaded from the OSM file
			//TODO: Make sure references from detailedLink -> processedLink are properly updated
			resolveDifferencesInDB(newDetailedMap, oldDetailedMap, start_time, fileName, description, false);

			//Mark the tile as complete
			synchronized(Controller.lock){
				tile.setDetailed_map_status(Tile.DONE);
				DBConnection.updateTile(tile);
			}
		}
		else if(processedBBoxes.size() > 0){

			long NOW = System.currentTimeMillis();

			//Get the newly parsed detailedMap from the queue
			BBox newProcessedMap = processedBBoxes.remove();

			//1) query the database for the current version of the map within the region spanned by the OSM file
			//If this is the first time this tile is added, this may contain a few links that go over the border, etc...
			long start_time = System.currentTimeMillis();
			BBox oldProcessedMap = DBConnection.boundingBoxQuery(newProcessedMap.leftLon, newProcessedMap.topLat, newProcessedMap.rightLon, newProcessedMap.bottomLat, NOW, true, true, true, true);

			String fileName = "";
			String description = "";
			Tile tile = newProcessedMap.getTile();
			if(tile!=null){
				fileName = tile.fileName();
				description = "Update of " + fileName;
			}

			//2) Compare our current DB version of the map against the new version loaded from the OSM file
			//TODO: Make sure references from detailedLink -> processedLink are properly updated
			resolveDifferencesInDB(newProcessedMap, oldProcessedMap, start_time, fileName, description, true);



			//Mark the tile as complete
			synchronized(Controller.lock){
				tile.setProcessed_map_status(Tile.DONE);
				DBConnection.updateTile(tile);
			}
			
			//TODO: Send Email

		}

		//Mark this thread as complete and start any new threads if necessary
		isComplete = false;
		Controller.startThreadsIfNecessary();
		
	}


	/**
	 * Tells whether the thread is finished running.  This can happen due to a successful run or an error.
	 * @return True if the thread completed successfully or failed, False if it is still running
	 */
	public boolean isFinished(){
		if(isComplete)
			return true;
		return !super.isAlive();
	}
}


