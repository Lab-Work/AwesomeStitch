package my.awesomestitch.mapobjects;

import java.sql.ResultSet;
import java.sql.SQLException;

import my.awesomestitch.control.Log;

import org.postgis.PGgeometryLW;

/**
 * This class represents the detail_link table in the DB.
 * There may be many detail_links for every link.
 * @author brian
 *
 */
public class DetailLink extends Link {
	/**
	 * Points to the regular link in the links table.  This will help us with visualization.
	 */
	long proc_link_id;

	/**
	 * Points to the OBJECT of a regular link in the links table.  This way, we can correctly retrieve the
	 * link_id as it is updated.
	 */
	Link proc_link = null;



	public DetailLink(ResultSet rs){
		super(rs);
		try{
			this.proc_link_id = rs.getLong("proc_link_id");
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}
	}

	public DetailLink(Node begin_node, Node end_node, String osm_name, String osm_class, long osm_way_id) {
		super(begin_node, end_node, osm_name, osm_class, osm_way_id);
	}

	public DetailLink(long linkId, long begin_node_id, long end_node_id, float begin_angle, float end_angle, String name){
		super(linkId, begin_node_id, end_node_id, begin_angle, end_angle, name);
	}

	public DetailLink(){
		super();
	}

	@Override
	public String getTableName(){
		return "tmp_schema.detail_links";
	}

	@Override
	public String getInsertStatement() {
		// TODO Auto-generated method stub
		//Build SQL query from the data within this link
		//Query has form:
		//INSERT INTO schema_name.link (link_id , begin_node_id , end_node_id , begin_angle , end_angle , osm_name ,
		//	osm_class , osm_way_id , osm_begin_node_id , osm_end_node_id , geom)
		//	VALUES(?,?,?,?,?,?,?,?,?,?,'LINESTRING(? ?,? ?)');

		//Single quotes ' cause problems for SQL, but they are escaped as ''
		//so we have to replace all instances of ' with '' before the insert statement will work.

		String tmp_osm_name;
		if(osm_name==null || osm_name.equals(""))
			tmp_osm_name = null;
		else
			tmp_osm_name = osm_name.replace("'", "''");

		String query = "INSERT INTO " + getTableName() + " (link_id , proc_link_id, begin_node_id , end_node_id , begin_angle , end_angle , street_length, osm_name ," +
				" osm_class , osm_way_id , geom, osm_changeset, birth_timestamp, death_timestamp) VALUES(" +
				id + "," + proc_link_id + "," + begin_node_id + "," + end_node_id +", " + begin_angle + ", " + end_angle + ", " +
				street_length + ", '" + tmp_osm_name +"','" + osm_class + "'," + osm_way_id + "," +
				"'SRID=4326;LINESTRING(" + geom.getPoint(0).x + " " + geom.getPoint(0).y + "," + geom.getPoint(1).x + " " + geom.getPoint(1).y + ")', " +
				osm_changeset + ", " + birth_timestamp + ", " + death_timestamp + ");";
		return query;
	}


	/**
	 * Returns an SQL query - this query will give us the highest link_id which exists in the table
	 */
	@Override
	public String getHighestIdQuery() {
		return "select max(link_id) from " + getTableName() + ";";
	}

	@Override
	public String getCSVLine() {
		//first get geometry data string for this object
		geom.srid=4326;
		PGgeometryLW tmp = new PGgeometryLW(geom);
		String geom_string = tmp.getValue();

		String line = id + "|" + proc_link_id + "|" + begin_node_id + "|" + end_node_id + "|" + begin_angle + "|" + end_angle + "|" +
				street_length + "|" + osm_name + "|" + osm_class + "|" + osm_way_id  + "|" + geom_string + "|" + osm_changeset + "|" +
				birth_timestamp + "|" + death_timestamp + "\n";
		return line;
	}





	public long getProc_link_id() {
		return proc_link_id;
	}

	public void setProc_link_id(long proc_link_id) {
		this.proc_link_id = proc_link_id;
	}
	
	
	public void updateProc_link_id(){
		if(this.proc_link!=null)
			this.proc_link_id = this.proc_link.getId();
		else
			this.proc_link_id = 0;
	}

	

	public Link getProc_link() {
		return proc_link;
	}

	public void setProc_link(Link proc_link) {
		this.proc_link = proc_link;
	}


}
