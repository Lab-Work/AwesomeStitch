package my.osmstitch.mapobjects;

import java.sql.ResultSet;

/**
 * Represents the detail_node class in the DB.  This is intended to be a large table of Nodes for visualization purposes:
 * There may be several nodes in between intersections to represent a curved road.
 * Divided swtreets are represented as two sets of links and nodes.
 * So an intersection of two divided streets will contain 4 detailed_nodes, but only 1 regular node
 * @author brian
 *
 */
public class DetailNode extends Node {

	
	@Override
	public String getTableName(){
		return "tmp_schema.detail_nodes";
	}
	
	public DetailNode(ResultSet rs){
		super(rs);
	}
	
	public DetailNode(){
		super();
	}
	
	public DetailNode(Node n){
		this.node_id = n.getId();
		this.is_complete = n.isComplete();
		this.osm_traffic_controller = n.getOsm_traffic_controller();
		this.osm_changeset = n.getOsm_changeset();
		this.birth_timestamp = n.getBirth_timestamp();
		this.death_timestamp = n.getDeath_timestamp();
		this.geom = n.getGeom();
	}
	
}
