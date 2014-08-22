package my.osmstitch.mapobjects;

import java.util.Hashtable;
import java.util.IdentityHashMap;


/**
 * Maps a set of detailed links to a smaller set of processed links.  This is useful in map preprocessing because the DetailLinks must have
 * references to their corresponding processed Links at the end.  For efficient mapping, a disjoint-set forest structure is used
 * (http://en.wikipedia.org/wiki/Disjoint-set_data_structure)
 * This allows us to efficiently maintain the mapping, even when we repeatedly join links.  Path compression is employed for further
 * optimization.
 * @author brian
 *
 */

public class DetailLinkMap {
	/**
	 * Since we are using an inverted-tree structure, the leaves are the starting point for this data structure.  The leafMap
	 * maps our original DetailLinks to the actual leaf nodes of this data structure.
	 */
	private IdentityHashMap<Link, ForestNode> leafMap;
	
	/**
	 * Default constructor.  Initializes the hashtable of leaf nodes.
	 */
	public DetailLinkMap(){
		leafMap = new IdentityHashMap<Link, ForestNode>();
	}
	
	/**
	 * Creates a new ForestNode which is disjoint from all of our other nodes.  This contains a single Link, which may be one of the
	 * original DetailLinks, or a new processedLink, created from joining two of them.
	 * @param link
	 */
	public void add(Link link){
		ForestNode fn = new ForestNode(link);
		leafMap.put(link,  fn);
	}
	
	/**
	 * Joins two trees in the disjoint set structure.  Should be called TWICE after two links are joined together into one, during
	 * map preprocessing - each old link (child) must be attached to the new link (parent).  Note that the role of childLink and 
	 * parentLink is very important - we are mapping DetailLinks to processed Links, not the other way around.
	 * @param childLink One of the two DetailLinks, which are eventually joined into one
	 * @param parentLink The new processed Link, which was created by joining two DetailLinks
	 */
	public void union(Link childLink, Link parentLink){
		//Get the ForestNodes which correspond to the two given Links
		ForestNode child = leafMap.get(childLink);
		ForestNode parent = leafMap.get(parentLink);
		
		//Attach the two trees
		//Path Compression - Instead of attaching fn1 directly to fn2, attach it to the ancestor of fn2
		//This ensures that the tree will be shallower and more efficient
		child.parent = parent.getAncestor();
	}
	
	
	
	/**
	 * Find the processed Link that corresponds to a DetailLink, by recursively following parent pointers.  Should only be called
	 * once map preprocessing is complete and union() has been appropriately called on all link merges.  All DetailLinks that form
	 * a straight section of road should return the same processed Link.
	 * @param detailLink A link from the original, unprocessed map
	 * @return The link from the new, processed map, which the given detailLink is a part of
	 */
	public Link findLink(Link detailLink){
		//First get the leaf node corresponding to this link
		ForestNode leaf = leafMap.get(detailLink);
		
		
		if(!leafMap.containsKey(detailLink))
			System.out.println("Not in here:" + detailLink.getBegin_node_id() + "," + detailLink.getEnd_node_id());
		
		//Next, get the root node that this leaf is attached to
		ForestNode root = leaf.getAncestor();
		
		//Finally, return the Link from that node
		return root.link;
	}
	
	
	
	
	/**
	 * Utility class which represents a node in the forest.  Has a pointer to its parent (inverted tree structure).
	 * @author brian
	 *
	 */
	public class ForestNode{
		/**
		 * Reference to the parent of this node.  Recursive calls allow us to reach the root of the tree.
		 */
		protected ForestNode parent = null;
		
		/**
		 * Points to a single Link from the OSM map.  At the leaves of our forest, these will be DetailLinks, but
		 * as these links are repeatedly joined during preprocessing, they will eventually reach a root which is
		 * a processed Link. 
		 */
		protected Link link = null;
		
		
		/**
		 * Basic constructor.  Creates a leaf ForestNode, which contains an original DetailLink.
		 * @param link
		 */
		public ForestNode(Link link){
			this.link = link;
		}
		
		
		/**
		 * Returns the deepest ancestor of a node (root of the tree), by recursively following parent pointers.
		 * If this node has no parent, then it is the ancestor (root).
		 * @return
		 */
		public ForestNode getAncestor(){
			//Base case - this node has no parent, so return it
			if(this.parent==null)
				return this;
			
			//Recursive case - return the ancestor of our parent
			this.parent = this.parent.getAncestor();
			//Path compression - attach this node directly to its ancestor (instead of indirectly through its parents and so on)
			//This ensures that the tree stays small and future operations will be fast
			return(this.parent);
		}
		
	}
}
