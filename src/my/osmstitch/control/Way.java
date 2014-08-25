package my.osmstitch.control;

import java.util.ArrayList;

import my.osmstitch.mapobjects.Node;



public class Way {
	public long id;
	public long changeset;
	public String name = null;
	public String street_class;
	public ArrayList<Node> nodes;

	public boolean oneWay;
	public boolean backwards;

	public Way(){
		nodes = new ArrayList();
		oneWay = false;
		backwards = false;

	}

	/**
	 * updates the effects of this way on its nodes
	 * these will be important when deciding which nodes will be inserted
	 */
	public void updateNodes(){

		if(!isImportant())
			return;



		for(int i = 0; i < nodes.size(); i++){
			//increment the count of ways that pass through this node by one
			Node n = nodes.get(i);
			if(n !=null){
				n.incrementOsm_num_ways();

				//if this node is either endpoint of the way, mark it as such
				if(i==0){
					n.setWayEnd(true);
				}
				if(i==nodes.size()-1){
					n.setWayEnd(true);
				}
			}
			else{
				//If a node is null, mark its neighbors as endpoints
				if(i>0 && nodes.get(i-1) !=null){
					nodes.get(i-1).setWayEnd(true);
				}

				if(i < nodes.size() - 1 && nodes.get(i+1) !=null){
					nodes.get(i+1).setWayEnd(true);
				}
			}
			

		}
		
	}


	public boolean isImportant(){
		if(street_class==null)
			return false;
		String[] blacklist = {"pedestrian","service","track","path","footway","cycleway","bridleway","steps"};

		String[] whitelist = {"motorway", "motorway_link"};
		for(String s : blacklist){
			if(street_class.equals(s))
				return false;
		}
		
		if(street_class.equals("motorw"))
		
		if(name!=null){
			return false;
		}

		return true;
	}

	public String toString(){
		String str = "(Way " + name + " " + id +"): ";
		for(Node node : nodes){
			if(node==null)
				str += "null,";
			else
				str += node.getId() + ",";
		}
		return str;
	}



}
