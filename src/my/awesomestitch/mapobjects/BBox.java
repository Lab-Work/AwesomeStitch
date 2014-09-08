package my.awesomestitch.mapobjects;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;

import my.awesomestitch.control.Log;

import org.postgis.LineString;
import org.postgis.Point;

import and.awt.geom.Line2D;



public class BBox {
	/**
	 * The size of the safety margin around any given BoundingBox.  The safety margin allows us to find
	 * Nodes which are not strictly within our box, but are nearby enough that they may be useful.
	 */
	public static final double SAFETY_MARGIN_SIZE = .01;


	/**
	 * A length in meters which describes the minimum allowed link length in the processed map.
	 * Links which are too short will be compressed into a single node - this will flatten
	 * divided highways into standard two-way streets for easy counting (only one node per intersection)
	 */
	public static final double MIN_LINK_LENGTH = 40;

	/**
	 * The latitude and longitude ranges, which are given when the box is created.
	 */
	public double leftLon, rightLon, topLat, bottomLat;


	/**
	 * Track the actual size of the BBox.  Each time a node is added, these values may change.  This allows
	 * us to determine if we have created too big of a box.
	 */
	public double min_lon, max_lon, min_lat, max_lat;

	long maxChangeSet = 0;



	/**
	 * The Nodes contained within a BoundingBox.  node_ids map to Node objects.
	 */
	public Hashtable<Long,Node> nodes;

	/**
	 * The Nodes contained within a "safety margin" around a BoundingBox. node_ids map to Node objects.
	 */
	public Hashtable<Long, Node> safeNodes;


	/**
	 * The CountingNodes contained within the box.
	 */
	public Hashtable<Long, CountingNode> countingNodes;


	/** The links contained within a BoundingBox.  (begin_node_id, end_node_id) map to Link objects.
	 */
	public Hashtable<List<Long>, Link> links;

	/** The links contained within a "safety margin" around a BoundingBox.
	 * (begin_node_id, end_node_id) map to Link objects.
	 */
	public Hashtable<List<Long>, Link> safeLinks;

	/**
	 * Stores Links by id number
	 */
	public Hashtable<Long, Link> linksById;

	/**
	 * Maps a node_id to the list of links that begin with this node (out Links)
	 */
	private Hashtable<Long, List<Link>> linksByBeginNode;

	/**
	 * Maps a node_id to the list of links that end with this node (in Links)
	 */
	private Hashtable<Long, List<Link>> linksByEndNode;

	/**
	 * A list of mappings between DetailLinks and processed Links
	 */
	private List<DetailLinkMapping> detailLinkMappings;


	/**
	 * A Hashtable for quick lookup of DetailLinkMappings
	 */
	private Hashtable<List<Long>, DetailLinkMapping> detailLinkMappingTable;

	/**
	 * Local data structure used during map preprocessing.  Stores the mapping from DetailLinks to processed Links.
	 */
	private DetailLinkMap linkMap = null;
	
	/**
	 * The tile that this BBox was constructed from
	 */
	private Tile tile = null;



	public Tile getTile() {
		return tile;
	}


	public void setTile(Tile tile) {
		this.tile = tile;
	}


	public BBox(double left, double top, double right, double bottom){
		this.leftLon = left;
		this.topLat = top;
		this.rightLon = right;
		this.bottomLat = bottom;


		this.min_lon = 1000;
		this.max_lon = -1000;
		this.min_lat = 1000;
		this.max_lat = -1000;

		nodes = new Hashtable<Long, Node>();
		safeNodes = new Hashtable<Long, Node>();
		countingNodes = new Hashtable<Long, CountingNode>();
		links = new Hashtable<List<Long>, Link>();
		safeLinks = new Hashtable<List<Long>, Link>();
	}


	/**
	 * Helper function.  Returns whether some node is geographically within the
	 * region specified by this BBox.  Note that this BBox may contain some Nodes which
	 * are geographically outside of the region (Safety Nodes).
	 * @return True if the node is within the given region, false if not.
	 */
	public boolean inBox(Node node){
		if(node==null)
			return false;

		if(node.getGeom()==null)
			return false;

		double x = node.getGeom().x;
		double y = node.getGeom().y;
		if(x > leftLon && x < rightLon && y < topLat && y > bottomLat)
			return true;
		return false;

	}


	private boolean inBox(Link link){
		boolean begin_in = false;
		double x1 = link.getGeom().getPoint(0).x;
		double y1 = link.getGeom().getPoint(0).y;
		if(x1 > leftLon && x1 < rightLon && y1 < topLat && y1 > bottomLat)
			begin_in = true;

		boolean end_in = false;
		double x2 = link.getGeom().getPoint(1).x;
		double y2 = link.getGeom().getPoint(1).y;
		if(x2 > leftLon && x2 < rightLon && y2 < topLat && y2 > bottomLat)
			end_in = true;

		if(begin_in || end_in)
			return true;


		//We may have a case where neither endpoints lie in the box, but the link still crosses the box
		//To test this, we see if the link crosses any of the four border lines of the box
		if(Line2D.linesIntersect(x1, y1, x2, y2, leftLon, bottomLat, leftLon, topLat))
			return true; //Intersects left edge
		if(Line2D.linesIntersect(x1, y1, x2, y2, leftLon, topLat, rightLon, topLat))
			return true; //Intersects top edge
		if(Line2D.linesIntersect(x1, y1, x2, y2, rightLon, topLat, rightLon, bottomLat))
			return true; //Intersects right edge
		if(Line2D.linesIntersect(x1, y1, x2, y2, rightLon, bottomLat, leftLon, bottomLat))
			return true; //Intersects bottom


		//Neither case satisfied - the link does not intersect this box
		return false;

	}

	private boolean inSafetyMargin(Node node){
		if(node==null)
			return false;
		double x = node.getGeom().x;
		double y = node.getGeom().y;
		if(x > leftLon - SAFETY_MARGIN_SIZE && x < rightLon + SAFETY_MARGIN_SIZE
				&& y < topLat + SAFETY_MARGIN_SIZE && y > bottomLat - SAFETY_MARGIN_SIZE)
			return true;
		return false;
	}

	/**
	 * Adds a node to the Bounding Box.  Depending on its position, may place it into nodes
	 * or safeNodes (a safety margin slightly outside of this box)
	 * @param node The node we are putting into the BoundingBox
	 */
	public void add(Node node){


		if(inBox(node))
			nodes.put(new Long(node.getId()), node);
		else
			safeNodes.put(new Long(node.getId()), node);

		if(node.getOsm_changeset() > maxChangeSet)
			maxChangeSet = node.getOsm_changeset();

		//Update the range of latitudes/longitudes that this box covers
		min_lon = Math.min(min_lon, node.getGeom().x);
		max_lon = Math.max(max_lon, node.getGeom().x);
		min_lat = Math.min(min_lat, node.getGeom().y);
		max_lat = Math.max(min_lat, node.getGeom().y);

	}

	/**
	 * Delete a node from this bbox
	 * @param node_id The id of the node in question.
	 */
	public void delete(long node_id){

		if(nodes.containsKey(node_id))
			nodes.remove(node_id);

		if(safeNodes.containsKey(node_id))
			safeNodes.remove(node_id);
	}

	/**
	 * Adds a CountingNode to the list.
	 * @param cn
	 */
	public void add(CountingNode cn){
		//System.out.println("Adding countingnode " + cn.getName() + "(" + cn.getGeom().x + "," + cn.getGeom().y + ")");
		countingNodes.put(cn.getId(), cn);
	}

	/**
	 * Adds a link to the BoundingBox.  Will only succeed if the required Nodes are
	 * already in the boundingBox
	 * @param link The Link we are putting into the Boundingbox
	 */
	public void add(Link link){
		boolean hasBeginNode = nodes.containsKey(link.getBegin_node_id()) || safeNodes.containsKey(link.getBegin_node_id());
		boolean hasEndNode = nodes.containsKey(link.getEnd_node_id()) || safeNodes.containsKey(link.getEnd_node_id());
		if(hasBeginNode && hasEndNode){
			List<Long> key = new LinkedList<Long>();
			key.add(link.getBegin_node_id());
			key.add(link.getEnd_node_id());

			//If both endpoints are in the safety margin, this is a "safe link" otherwise, it is a regular link
			/*
			if(safeNodes.containsKey(link.getBegin_node_id()) && safeNodes.containsKey(link.getEnd_node_id())){
				safeLinks.put(key, link);
			}
			else{
				links.put(key, link);
			}*/

			if(!inBox(link)){
				safeLinks.put(key, link);
			}
			else{
				links.put(key, link);
				//If the link is put into the regular box, so must its endnodes.
				if(safeNodes.containsKey(link.getBegin_node_id())){
					Node n = safeNodes.remove(link.getBegin_node_id());
					nodes.put(link.getBegin_node_id(), n);
				}

				if(safeNodes.containsKey(link.getEnd_node_id())){
					Node n = safeNodes.remove(link.getEnd_node_id());
					nodes.put(link.getEnd_node_id(), n);
				}

			}

			if(link.getOsm_changeset() > maxChangeSet)
				maxChangeSet = link.getOsm_changeset();


		}
		else{/*
			if(!hasBeginNode)
				System.out.println("Didn't have required node " + link.getBegin_node_id() + " -- couldn't insert " + link);
			if(!hasEndNode)
				System.out.println("Didn't have required node " + link.getEnd_node_id() + " -- couldn't insert " + link);
		 */
		}
	}

	/**
	 * Delete a Link from this bbox
	 * @param link the Link in question.
	 */
	public void delete(Link link){
		List<Long> key = new LinkedList<Long>();
		key.add(link.getBegin_node_id());
		key.add(link.getEnd_node_id());

		if(links.containsKey(key)){
			links.remove(key);
		}
		else if(safeLinks.containsKey(key)){
			safeLinks.remove(key);
		}
		else
			System.out.println("Couldn't delete! No such link as " + link);
	}


	/**
	 * Lookup a node by id.
	 * @param nodeId The id of the node we want
	 * @return a Node object which has the given id, or null if no such Node exists.
	 */
	public Node getNode(long nodeId){
		if(nodes.containsKey(nodeId))
			return nodes.get(nodeId);

		if(safeNodes.containsKey(nodeId))
			return safeNodes.get(nodeId);

		return null;
	}

	/**
	 * Lookup a link by its begin and end nodes
	 * @param begin_node_id The id of the node this link starts at
	 * @param end_node_id The id of the node this link ends at
	 * @return The link which connects the two nodes, or null if no such Link exists.
	 */
	public Link getLink(long begin_node_id, long end_node_id){
		List<Long> key = new LinkedList<Long>();
		key.add(begin_node_id);
		key.add(end_node_id);

		if(links.containsKey(key))
			return links.get(key);
		else
			return safeLinks.get(key);

	}

	/**
	 * Determines whether the BBox contains some node.  Ignores nodes in the safety margin.
	 * @param nodeId The ID of the node in question
	 * @return True if the BBox contains the node, False if it does not
	 */
	public boolean hasNode(long nodeId){
		return nodes.containsKey(nodeId);
	}


	/**
	 * Determines whether the BBox contains some CountingNode.
	 * @param cn_id The id of the counting node in question (a negative number)
	 * @return True if the BBox contains the CountingNode, False if it does not.
	 */
	public boolean hasCountingNode(long cn_id){
		return countingNodes.containsKey(cn_id);
	}

	/**
	 * Determines whether the BBox contains some node, including some safety margin
	 * around the box.  This is useful in case a node has been moved slightly outside
	 * of the box in a recent OSM update.  We can still compare nodes between the DB
	 * and OSM even if they have been moved slightly.
	 * @param nodeId The ID of the node in question
	 * @return True if the BBox (including the safety margin) contains the node, False if it does not
	 */
	public boolean safeHasNode(long nodeId){
		return nodes.containsKey(nodeId) || safeNodes.containsKey(nodeId);
	}


	/**
	 * Determines whether the BBox contains some Link. This is true if the BBox contains either
	 * the begin_node or the end_node
	 * @param begin_node_id The node where the Link starts.
	 * @param end_node_id The node where the Link ends.
	 * @return True if the BBox contains the Link, False if it does not
	 */
	public boolean hasLink(long begin_node_id, long end_node_id){
		List<Long> key = new LinkedList<Long>();
		key.add(begin_node_id);
		key.add(end_node_id);
		return links.containsKey(key);
		//return nodes.containsKey(begin_node_id) || nodes.containsKey(end_node_id);
	}

	/**
	 * Determines whether the BBox contains some Link, even if both the begin_node and end_node
	 * are contained in the safety margin.
	 * @param begin_node_id The node where the Link starts.
	 * @param end_node_id The node where the Link ends.
	 * @return
	 */
	public boolean safeHasLink(long begin_node_id, long end_node_id){
		List<Long> key = new LinkedList<Long>();
		key.add(begin_node_id);
		key.add(end_node_id);
		return links.containsKey(key) || safeLinks.containsKey(key);
	}

	public Link getLinkById(long id){
		if(linksById==null){
			linksById = new Hashtable<Long, Link>();
			for(Link link : getAllLinks()){
				linksById.put(link.getId(), link);
			}
		}

		return linksById.get(id);
	}


	public DetailLinkMapping lookupDLM(long link_id, long detailed_link_id){

		
		if(this.detailLinkMappingTable==null){
			this.detailLinkMappingTable = new Hashtable<List<Long>, DetailLinkMapping>();
			if(this.detailLinkMappings==null)
				return null;
			for(DetailLinkMapping dlm : this.detailLinkMappings){
				List<Long> key = new LinkedList<Long>();
				key.add(dlm.getLink_id());
				key.add(dlm.getDetail_link_id());
				detailLinkMappingTable.put(key, dlm);
			}
		}

		List<Long> key = new LinkedList<Long>();
		key.add(link_id);
		key.add(detailed_link_id);

		return this.detailLinkMappingTable.get(key);
	}


	public boolean hasDLM(long link_id, long detailed_link_id){

		DetailLinkMapping dlm = this.lookupDLM(link_id, detailed_link_id);


		return (dlm!=null);

	}



	/**
	 * Generates a List which contains all Nodes, from both the main portion of the box,
	 * and theh safety margin.
	 * @return A List of all Nodes.
	 */
	public List<Node> getAllNodes(){
		List<Node> nodeList = new LinkedList<Node>();
		for(long id : nodes.keySet()){
			Node n = nodes.get(id);
			if(n!=null)
				nodeList.add(n);
		}

		for(long id : safeNodes.keySet()){
			Node n = safeNodes.get(id);
			if(n!=null)
				nodeList.add(n);
		}
		return nodeList;
	}


	/**
	 * Generates a list which contains all Links, from both the main portion of the box,
	 * and the safety margin.
	 * @return A List of all Links.
	 */
	public List<Link> getAllLinks(){
		List<Link> list = new LinkedList<Link>();
		for(List<Long> link_ids : links.keySet()){
			//loop through all Links in the bounding box
			long begin_node_id = link_ids.get(0);
			long end_node_id = link_ids.get(1);
			Link link = getLink(begin_node_id, end_node_id);
			list.add(link);
		}

		for(List<Long> link_ids : safeLinks.keySet()){
			//also loop through Links in the safety margin
			long begin_node_id = link_ids.get(0);
			long end_node_id = link_ids.get(1);
			Link link = getLink(begin_node_id, end_node_id);
			list.add(link);
		}

		return list;
	}

	/**
	 * Generates a list which contains all CountingNodes within this box.
	 * @return a List of all Counting Nodes. This list will be empty if there are no Counting Nodes.
	 */
	public List<CountingNode> getAllCountingNodes(){
		List<CountingNode> list = new LinkedList<CountingNode>();
		if(countingNodes==null)
			return list;

		for(long id : countingNodes.keySet()){
			CountingNode cn = countingNodes.get(id);
			list.add(cn);
		}
		return list;
	}


	/**
	 * Returns all of the links in the BBox that flow into the given node (have this node as an end node).
	 * @param node_id The id of the node in question.
	 * @return A List of relevant Links
	 */
	public List<Link> getInLinks(long node_id){
		if(linksByEndNode==null)
			generateLinkTables();

		if(linksByEndNode.containsKey(node_id))
			return linksByEndNode.get(node_id);
		else
			return new LinkedList<Link>();
	}

	/**
	 * Returns all of the links in the BBox that flow out of the given node (have this node as a begin node)
	 * @param node_id The id of the node in questioin.
	 * @return A List of relevant LInks
	 */
	public List<Link> getOutLinks(long node_id){
		if(linksByBeginNode==null)
			generateLinkTables();

		if(linksByBeginNode.containsKey(node_id))
			return linksByBeginNode.get(node_id);
		else
			return new LinkedList<Link>();
	}




	/**
	 *  Generates hash tables, which make it easy to look links up by their begin_node or end_node
	 *  These hash tables, respectively, map begin_node_id-->List of links starting from this node
	 *  and end_node_id-->List of links ending at this node.
	 * @param box A BBox containing some links and nodes
	 */
	public void generateLinkTables(){
		linksByBeginNode = new Hashtable<Long, List<Link>>();
		linksByEndNode = new Hashtable<Long, List<Link>>();
		for(Link link : getAllLinks()){
			//loop through all links in the bounding box
			long begin_node_id = link.getBegin_node_id();
			long end_node_id = link.getEnd_node_id();

			//Map the begin_node_id to a list of Links which start at this node
			List<Link> list = linksByBeginNode.get(begin_node_id);
			if(list==null){
				list = new LinkedList<Link>();
				linksByBeginNode.put(begin_node_id, list);
			}
			list.add(getLink(begin_node_id, end_node_id));

			//Map the end_node_id to a list of Links which end at this node
			list = linksByEndNode.get(end_node_id);
			if(list==null){
				list = new LinkedList<Link>();
				linksByEndNode.put(end_node_id, list);
			}

			list.add(getLink(begin_node_id, end_node_id));

		}
	}

	/**
	 * When a link is removed (for example during preprocessing),
	 * we must update the hashtables that keep track of links by begin and end node.
	 * This function allows us to do it without having to regenerate the entire table each time.
	 * @param link The link to be deleted
	 */
	private void removeFromTables(Link link){
		List<Link> beginLinks = linksByBeginNode.get(link.getBegin_node_id());
		if(beginLinks !=null && link!=null)
			beginLinks.remove(link);
		List<Link> endLinks = linksByEndNode.get(link.getEnd_node_id());
		if(endLinks !=null && link!=null)
			endLinks.remove(link);
	}

	private void removeAllFromTables(Node n){
		if(linksByBeginNode.contains(n.getId()))
			linksByBeginNode.remove(n.getId());

		if(linksByEndNode.contains(n.getId()))
			linksByEndNode.remove(n.getId());

	}

	/**
	 * When a link is added (for example during preprocessing)
	 * we must update the hashtables that keep track of links by begin and end node.
	 * This function allows us to do it without having to regenerate the entire table each time.
	 * @param link The link to be deleted
	 */
	private void addToTables(Link link){
		List<Link> beginLinks = linksByBeginNode.get(link.getBegin_node_id());
		if(beginLinks==null){
			beginLinks = new LinkedList<Link>();
			linksByBeginNode.put(link.getBegin_node_id(), beginLinks);
		}
		if(!beginLinks.contains(link))
			beginLinks.add(link);

		List<Link> endLinks = linksByEndNode.get(link.getEnd_node_id());
		if(endLinks==null){
			endLinks = new LinkedList<Link>();
			linksByEndNode.put(link.getEnd_node_id(), endLinks);
		}
		if(!endLinks.contains(link))
			endLinks.add(link);
	}

	public static void printTable(Hashtable<Long, List<Link>> table){
		for(Long id : table.keySet()){
			System.out.println(id + "=====>" + table.get(id));
		}
	}

	/**
	 * In a given BBox, collapse all links which are shorter than MIN_LINK_LENGTH into single nodes.
	 * Effectively, this will transform divided highways into a normal two-way street,
	 * making it easier to count at (only one node per intersection).
	 * @param box A BBox, which has been loaded with Links and Nodes.  On all Links, link lengths must already
	 * be set with Link.setStreet_length()
	 * @return The number of links that have been compressed
	 */
	public int collapseShortLinks(){
		//Step 1) Create hash tables, which make it easy to look links up by their begin_node or end_node
		//Eventually, we are going to start deleting nodes, and we will want an efficient way to look up and
		//modify the affected links
		if(linksByBeginNode==null || linksByEndNode==null)
			generateLinkTables();



		//Step 2) Identify all links which are too short
		//These links will be collapsed into a single node, and affected Links will be updated
		List<Link> links = this.getAllLinks();

		int numchanged = 0;
		int iter = 1;
		for(Link link : links){
			if(link.getStreet_length() < MIN_LINK_LENGTH && this.hasLink(link.getBegin_node_id(), link.getEnd_node_id())){
				try{
					/*
				System.out.println("*Begin");
				printTable(linksByBeginNode);
				System.out.println("*End");
				printTable(linksByEndNode);
				System.out.println("*Box");
				box.printLinks();
				System.out.println("*");

				System.out.println("Removing " + link);
					 */


					//delete this link
					this.delete(link);
					removeFromTables(link);

					//also delete its counterpart if the street is two-way
					Link otherLink = this.getLink(link.getEnd_node_id(), link.getBegin_node_id());
					if(otherLink != null){
						this.delete(otherLink);
						removeFromTables(otherLink);
					}


					//We will keep one of the links endpoint nodes, and delete the other
					//Other links may point to this delete_node - we must modify them to point to the keep_node
					long keep_node = link.getBegin_node_id();

					long delete_node = link.getEnd_node_id();

					//System.out.println("Deleting node " + delete_node + " , keeping node " + keep_node);
					Node kNode = this.getNode(keep_node);
					if(kNode!=null){
						//Update affected Links whose begin_node is the one we are deleting
						List<Link> affectedOutLinks = new LinkedList<Link>();
						if(linksByBeginNode.get(delete_node)!=null)
							affectedOutLinks = new LinkedList<Link>(linksByBeginNode.get(delete_node));
						for(Link affectedLink : affectedOutLinks){
							this.delete(affectedLink);
							removeFromTables(affectedLink);
							affectedLink.setBegin_node_id(keep_node);
							//also update this Link's geometry, since it has changed
							Point oldEndPoint = affectedLink.getGeom().getPoint(1);
							Point[] points = {kNode.getGeom(), oldEndPoint};
							LineString newGeom = new LineString(points);
							affectedLink.setGeom(newGeom);
							this.add(affectedLink);
							addToTables(affectedLink);

						}
						//finally, update our hashtable - affected links now have a different begin_node
						linksByBeginNode.remove(delete_node);

						//Update affected Links whose end_node is the one we are deleting
						List<Link> affectedInLinks = new LinkedList<Link>();
						if(linksByEndNode.get(delete_node)!=null)
							affectedInLinks = new LinkedList<Link>(linksByEndNode.get(delete_node));
						for(Link affectedLink : affectedInLinks){
							this.delete(affectedLink);
							removeFromTables(affectedLink);
							affectedLink.setEnd_node_id(keep_node);
							//also update this Link's geometry, since it has changed
							Point oldBeginPoint = affectedLink.getGeom().getPoint(0);
							Point[] points = {oldBeginPoint, kNode.getGeom()};
							LineString newGeom = new LineString(points);
							affectedLink.setGeom(newGeom);
							this.add(affectedLink);
							addToTables(affectedLink);
						}
						//finally, update our hashtable - affected links now have a different end_node
						linksByEndNode.remove(delete_node);



						//finally, delete the node
						this.delete(delete_node);
					}
				}
				catch(Exception e){
					e.printStackTrace();
				}

			}
		}

		return 0;

	}



	/**
	 * Determine if all inLinks have a matching outLink and vice versa.  By "matching" we mean
	 * that the two links are the corresponding parts of a two-way street.  In other words
	 * if link1.begin_node = link2.end_node and link1.end_node = link2.begin_node, they match
	 * @param inLinks The List of links flowing into a node
	 * @param outLinks The List of links flowing out of a node
	 * @return True if each inLink has a matching outLink has a matching inLink.  False if any link does not have a match.
	 */
	public static boolean allLinksHaveMatch(List<Link> inLinks, List<Link> outLinks){

		//First check that all inLinks have a match.  If any inLink does not have a match, return false
		for(Link in : inLinks){
			boolean foundMatch = false;
			for(Link out : outLinks){
				if(in.getBegin_node_id()==out.getEnd_node_id() && in.getEnd_node_id()==out.getBegin_node_id())
					foundMatch = true;
			}
			if(!foundMatch)
				return false;
		}

		//Next check that all outLinks have a match.  If any outLink does not have a match, return false
		for(Link out : outLinks){
			boolean foundMatch = false;
			for(Link in : inLinks){
				if(in.getBegin_node_id()==out.getEnd_node_id() && in.getEnd_node_id()==out.getBegin_node_id())
					foundMatch = true;
			}
			if(!foundMatch)
				return false;
		}


		//Didn't find any mismatches - return true
		return true;
	}


	/**
	 * Tells whether some node is the end of a way.  These nodes are important, even if
	 * they are not an intersection.  This is done by examining the in- and out-links
	 * attached to the node.
	 * @param n The node in question
	 * @return True if it is the end of the way, False if it is not.
	 */
	public boolean isWayEnd(Node n){

		//We want to calculate the degree of this node - if it is 1, the node is a way end
		//However, if we have a two-way street, there will be 2 links for the same street - we do not want to double-count these

		List<Link> inLinks = getInLinks(n.getId());
		List<Link> outLinks = getOutLinks(n.getId());

		HashSet<List<Long>> linkSet = new HashSet<List<Long>>();

		//Add all in-links to the set
		for(Link l : inLinks){
			//Link is encoded as (begin_node, end_node) pair
			List<Long> idKey = new LinkedList<Long>();
			idKey.add(l.getBegin_node_id());
			idKey.add(l.getEnd_node_id());
			//Add to the set
			linkSet.add(idKey);
		}

		for(Link l : outLinks){
			//Link is encoded as (end_node begin_node) pair
			//Note that these are backwards from inLinks
			//so the matching inLink and outLink will get the same encoding
			List<Long> idKey = new LinkedList<Long>();
			idKey.add(l.getEnd_node_id());
			idKey.add(l.getBegin_node_id());
			//Add to the set
			linkSet.add(idKey);
		}

		//The size of the set = the degree of the node
		if(linkSet.size()==2)
			return false;
		else
			return true;
	}

	/**
	 * Collapses a Node, joining the links on either side into a single link.  For example,
	 * -->o--> will become ------>, and <==>o<==> will become <=======>.
	 * This is useful for preprocessing a detailed map.  This should only be called on a non-intersection Node
	 * otherwise the behavior may be unexpected.
	 * @param n The Node we want to collapse
	 */
	public void collapseNode(Node n){
		//Get the links that flow in and out of this node
		List<Link> inLinks = new LinkedList<Link>(getInLinks(n.getId()));
		List<Link> outLinks = getOutLinks(n.getId());

		//If the linkMap does not exist yet, create it
		if(this.linkMap==null){
			this.linkMap = new DetailLinkMap();
			for(Link link : this.getAllLinks())
				this.linkMap.add(link);
		}

		//Loop through the inLinks of this node - each one must be joined with an outLink		
		for(Link in : inLinks){
			//Discover the "next" outLink which corresponds to this inLink
			//This is the link which continues the path of this link, so the two should be joined
			Link out = null;
			for(Link o : outLinks){
				if(in.getBegin_node_id() != o.getEnd_node_id()){
					out = o;
					break;
				}
			}
			if(out!=null){
				//remove the two old links
				this.removeFromTables(in);
				this.removeFromTables(out);
				this.delete(in);
				this.delete(out);
				//Create the new link which connects them
				Link joined = new Link(0, in.getBegin_node_id(), out.getEnd_node_id(), in.getBegin_angle(), out.getEnd_angle(), in.getOsm_name());
				//Copy other attributes from the old links
				joined.setStreet_length(in.getStreet_length() + out.getStreet_length());
				Point[] points = {in.getGeom().getPoint(0), out.getGeom().getPoint(1)};
				LineString geom = new LineString(points);
				joined.setGeom(geom);
				joined.setOsm_changeset(in.getOsm_changeset());
				joined.setOsm_class(in.getOsm_class());
				joined.setOsm_way_id(in.getOsm_way_id());
				//Add the new link to this BBox
				this.add(joined);
				this.addToTables(joined);


				//Maintain mapping from DetailLinks to processed Links
				linkMap.add(joined); 		//Add the new link to the linkMap
				linkMap.union(in, joined);	//Map the two small links to the new joined link
				linkMap.union(out, joined); //...

			}
		}

		//Finally, remove the node
		this.delete(n.getId());
		this.removeAllFromTables(n);
	}

	/**
	 * Collapses the nodes in the middle of a long way, so
	 *                 o
	 *                 |
	 * o---o---o---o---+---o---o---o
	 *                 |
	 *                 o
	 * becomes          
	 *                 o
	 *                 |
	 * o---------------+-----------o
	 *                 |
	 *                 o
	 * This allows us to have a much cleaner map to collect and process data on.
	 */
	public DetailLinkMap collapseTwowayNodes(){
		int iteration = 0;
		//Maps way Ids to the affected Nodes
		Hashtable<Long, HashSet<Long>> wayNodes = new Hashtable<Long, HashSet<Long>>();

		//STEP 1)
		//Count the number of ways that cross through each Node

		System.out.println("Counting intersections.");
		//Initialize counts to 0
		List<Node> allNodes = this.getAllNodes();
		for(Node node : allNodes){
			node.setOsm_num_ways((short) 0);
		}
		//Loop through all links, and use the way id to grow the set of nodes for this way
		//Note that Hashset guarantees there will be no duplicates, even if multiple links in a way reference the same node
		for(Link link : this.getAllLinks()){
			long way_id = link.getOsm_way_id();

			HashSet<Long> nodeSet = wayNodes.get(way_id);
			if(nodeSet==null){
				nodeSet = new HashSet<Long>();
				wayNodes.put(way_id, nodeSet);
			}
			nodeSet.add(link.getBegin_node_id());
			nodeSet.add(link.getEnd_node_id());			
		}
		//Loop through these ways and increment the count for each node
		for(Long way_id : wayNodes.keySet()){
			HashSet<Long> nodeSet = wayNodes.get(way_id);
			for(long node_id : nodeSet){
				Node n = this.getNode(node_id);
				n.incrementOsm_num_ways();
			}
		}

		System.out.println("Merging nodes.");


		//Step 2)
		//Nodes which have a way count of 1 are not important nodes, unless they are a way end
		//So we remove them and join the links on either side
		int i = 0;
		for(Node n : allNodes){
			i++;
			if(i%1000==0)
				System.out.println(i + " / " + allNodes.size());

			if(n.getOsm_num_ways() <= 1 && !isWayEnd(n)){
				this.collapseNode(n);
			}
		}

		//Return the link mapping, which results from collapsing these links
		return this.linkMap;
	}

	/**
	 * Creates a preprocessed copy of this map, which is much more concise.  This is done by taking long chains
	 * of Links and merging them into a single Link.  Also, divided highways are merged into a single two-way street, when possible.
	 * 
	 * This method also has an effect on the detailed map.  Each link in the detailed map will now contain a reference to the corresponding
	 * link in the concise map.
	 */
	public BBox preprocess(){
		//Copy this BBox - note that the new box has Nodes and Links, even if this box has DetailNodes or DetailLinks
		BBox processed = new BBox(this.leftLon, this.topLat, this.rightLon, this.bottomLat);
		processed.setTile(this.getTile());
		
		IdentityHashMap<Link,Link> correspondingLink = new IdentityHashMap<Link,Link>();
		System.out.println("Copying...");
		for(Node n : getAllNodes()){
			processed.add(new Node(n));
		}

		for(Link l: getAllLinks()){
			Link tmp = new Link(l);
			processed.add(tmp);
			correspondingLink.put(l, tmp);
		}

		//Collapse nodes which form a two-way street (unnecessary detail)
		//mapping will tell which links in the processed map correspond to which links in the detailed map
		System.out.println("Collapsing nodes.");
		DetailLinkMap mapping = processed.collapseTwowayNodes();

		//Also collapse extremely short Links (usually has the effect of merging divided highways)
		processed.collapseShortLinks();

		System.out.println("Collapsing links.");
		List<DetailLinkMapping> dlmList = new LinkedList<DetailLinkMapping>();

		System.out.println("Updating detailed link mapping");
		//Finally, use the mapping to update Link references within the DetailedMap
		for(Link detailed : this.getAllLinks()){
			//For each link in the detailed map, search the mapping for the corresponding processed Link
			Link tmp = correspondingLink.get(detailed);
			Link processedLink = mapping.findLink(tmp);

			//create a list of these mappings
			DetailLinkMapping dlm = new DetailLinkMapping(processedLink, detailed);

			if(processedLink==detailed){
				System.out.println("MATCH!");
				System.out.println(processedLink);
			}

			dlmList.add(dlm);
		}

		//save the list of mappings inside the processed BBox
		processed.setDetailLinkMappings(dlmList);


		return processed;

	}



	public long getMaxChangeSet() {
		return maxChangeSet;
	}

	public String toString(){
		String str = nodes.size() + " nodes, " + safeNodes.size() + " safeNodes, ";
		str += links.size() + " links, " + safeLinks.size() + " safeLinks";
		return str;
	}

	public void printLinks(){
		for(List<Long> key : links.keySet()){
			System.out.println(key + "~~>" + links.get(key));
		}
	}

	/**
	 * Saves this BBox to CSV files for easy use at a later time.
	 * @param nodeFileName The file where a table of Nodes will be saved
	 * @param linkFileName The file where a table of Links will be saved
	 */
	public void saveToFile(String nodeFileName, String linkFileName){
		
		try {
			File f = new File(nodeFileName);
			PrintWriter writer = new PrintWriter(f);
			writer.println("id,lat,lon,version");
			for(Node node : getAllNodes()){
				String line = node.getId() + "," + node.getGeom().y + "," + node.getGeom().x + "," + node.getBirth_timestamp();
				writer.println(line);
			}
			
		} catch (FileNotFoundException e) {
			Log.e(e);
		}
		
		try {
			File f = new File(linkFileName);
			PrintWriter writer = new PrintWriter(f);
			writer.println("id,begin_node_id,end_node_id,street_name,street_class,street_length,begin_angle,end_angle,begin_lat,begin_lon,end_lat,end_lon,version");
			for(Link link : getAllLinks()){
				String line = link.getId() + "," + link.getBegin_node_id() + "," + link.getEnd_node_id() + "," + link.getOsm_name().replace(",", "") +
						"," + link.getOsm_class() + "," + link.getStreet_length() + "," + link.getBegin_angle() + "," + link.getEnd_angle() + 
						"," + link.getGeom().getPoints()[0].y + ","  + link.getGeom().getPoints()[0].x +
						"," + link.getGeom().getPoints()[1].y + ","  + link.getGeom().getPoints()[1].x + "," + link.getBirth_timestamp();
				writer.println(line);
			}
			
		} catch (FileNotFoundException e) {
			Log.e(e);
		}
	}
	
	
	public double getMin_lon() {
		return min_lon;
	}

	public double getMax_lon() {
		return max_lon;
	}

	public double getMin_lat() {
		return min_lat;
	}

	public double getMax_lat() {
		return max_lat;
	}


	public List<DetailLinkMapping> getDetailLinkMappings() {
		return detailLinkMappings;
	}


	public void setDetailLinkMappings(List<DetailLinkMapping> detailLinkMappings) {
		this.detailLinkMappings = detailLinkMappings;
	}


}
