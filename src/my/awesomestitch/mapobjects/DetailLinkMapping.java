package my.awesomestitch.mapobjects;

import java.sql.ResultSet;
import java.sql.SQLException;

import my.awesomestitch.control.Log;

import org.postgis.LineString;
import org.postgis.PGgeometryLW;


public class DetailLinkMapping extends DBObject {

	/**
	 * The id of the processed link.
	 */
	private long link_id;

	private Link processed;


	/**
	 * The id of the detailed link.
	 */
	private long detail_link_id;

	private Link detailed;


	/**
	 * The Way Id, imported from OSM
	 */
	private long osm_way_id;


	/**
	 * The timestamp when this DetailLinkMapping was first created. (used for versioning)
	 */
	private long birth_timestamp;

	/**
	 * The timestamp when this DetailLinkMapping ceases to exist (used for versioning)
	 */
	private long death_timestamp;





	public DetailLinkMapping(){};

	/*
	public DetailLinkMapping(long link_id, long detail_link_id){
		this.link_id = link_id;
		this.detail_link_id = detail_link_id;
	}*/

	public DetailLinkMapping(Link processed, Link detail){
		this.link_id = processed.getId();
		this.detail_link_id = detail.getId();

		this.processed = processed;

		this.detailed = detail;

		this.osm_way_id = detail.getOsm_way_id();
	}

	public DetailLinkMapping(ResultSet rs){
		try{
			this.link_id = rs.getLong("link_id");
			this.detail_link_id = rs.getLong("detail_link_id");
			this.osm_way_id = rs.getLong("osm_way_id");
			this.birth_timestamp = rs.getLong("birth_timestamp");
			this.death_timestamp = rs.getLong("death_timestamp");
		}
		catch(SQLException e){
			Log.v("DB", "Error constructing DetailLinkMapping");
			Log.v("DB", e.getSQLState());
			Log.e(e);
		}
	}


	@Override
	public String getTableName() {
		return "tmp_schema.detail_link_mapping";
	}

	@Override
	public void setId(long id) {
		//Do nothing - not useful for this type of object
		return;
	}

	@Override
	public String getInsertStatement() {
		//Update ids before we insert
		if(this.processed!=null){
			this.link_id = this.processed.getId();
			this.detail_link_id = this.detailed.getId();
		}
		//String geom_string = "'SRID=4326;LINESTRING(" + geom.getPoint(0).x + " " + geom.getPoint(0).y + "," + geom.getPoint(1).x + " " + geom.getPoint(1).y + ")'";

		String sql = "INSERT INTO " + new DetailLinkMapping().getTableName() + 
				" (link_id, detail_link_id, osm_way_id, geom, birth_timestamp, death_timestamp) " +
				"VALUES (" + link_id + ", " + detail_link_id + ", " + osm_way_id + ", " + birth_timestamp + ", " + death_timestamp +
				");";

		return sql;
	}

	@Override
	public String getHighestIdQuery() {
		//Do nothing - not useful for this type of object
		return null;
	}

	@Override
	public String getCSVLine() {
		//geom.srid=4326;
		//PGgeometryLW lw = new PGgeometryLW(geom);
		//String geom_string = lw.getValue();
		//Update ids before we insert
		if(this.processed!=null){
			this.link_id = this.processed.getId();
			this.detail_link_id = this.detailed.getId();
			//System.out.println("not null");
		}
		else{
			//System.out.println("null");
		}
		String csv = link_id + "|" + detail_link_id + "|" + osm_way_id + "|" +
				birth_timestamp + "|" + death_timestamp + "\n";

		return csv;
	}

	@Override
	public int getServerBufferSize() {
		return 10;
	}

	public long getLink_id() {
		if(this.processed!=null){
			this.link_id = this.processed.getId();
		}
		return link_id;
	}

	public void setLink_id(long link_id) {
		this.link_id = link_id;
	}

	public long getDetail_link_id() {
		if(this.detailed!=null){
			this.detail_link_id = this.detailed.getId();
			//System.out.println("not null");
		}
		return detail_link_id;
	}

	public void setDetail_link_id(long detail_link_id) {
		this.detail_link_id = detail_link_id;
	}

	public long getOsm_way_id() {
		return osm_way_id;
	}

	public void setOsm_way_id(long osm_way_id) {
		this.osm_way_id = osm_way_id;
	}


	public long getBirth_timestamp() {
		return birth_timestamp;
	}

	public void setBirth_timestamp(long birth_timestamp) {
		this.birth_timestamp = birth_timestamp;
	}

	public long getDeath_timestamp() {
		return death_timestamp;
	}

	public void setDeath_timestamp(long death_timestamp) {
		this.death_timestamp = death_timestamp;
	}

	public Link getProcessed() {
		return processed;
	}

	public Link getDetailed() {
		return detailed;
	}

}
