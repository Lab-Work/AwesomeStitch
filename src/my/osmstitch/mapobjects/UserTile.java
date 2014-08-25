package my.osmstitch.mapobjects;

import java.sql.ResultSet;
import java.sql.SQLException;

import my.osmstitch.control.Log;

import my.osmstitch.mapobjects.DBObject;

public class UserTile extends DBObject{
	
	public static final long DNE = -1;
	
	private long user_id;
	
	private int grid_x;
	private int grid_y;
	
	private long ordered_timestamp = DNE;
	private long owned_timestamp = DNE;
	
	public UserTile(long user_id, int grid_x, int grid_y){
		this.user_id = user_id;
		this.grid_x = grid_x;
		this.grid_y = grid_y;
	}
	
	
	public UserTile(ResultSet rs){
		try{
			this.user_id = rs.getLong("user_id");
			this.grid_x = rs.getInt("grid_x");
			this.grid_y = rs.getInt("grid_y");
			this.owned_timestamp = rs.getLong("owned_timestamp");
			this.ordered_timestamp = rs.getLong("ordered_timestamp");
		}
		catch(SQLException e){
			Log.v("DB","SQL EXCEPTION");
			Log.v("DB",e.getSQLState());
			Log.e(e);
		}
	}
	
	public UserTile(){}
	
	
	@Override
	public String getTableName() {
		return "tmp_schema.user_tiles";
	}

	@Override
	public void setId(long id) {
		//Unused
		return;
	}

	@Override
	public String getInsertStatement() {
		String sql = "INSERT INTO " + getTableName() + " (user_id, grid_x, grid_y, ordered_timestamp, owned_timestamp)"
				+ " VALUES(" + user_id + "," + grid_x + "," + grid_y + "," + ordered_timestamp + "," + owned_timestamp + ");";
		return sql;
	}

	@Override
	public String getHighestIdQuery() {
		//Unused
		return null;
	}

	@Override
	public String getCSVLine() {
		String csv = user_id + "|" + grid_x + "|" + grid_y + "|" + ordered_timestamp + "|" + owned_timestamp + "\n";
		return csv;
	}

	@Override
	public int getServerBufferSize() {
		return 10;
	}

	public long getUser_id() {
		return user_id;
	}

	public void setUser_id(long user_id) {
		this.user_id = user_id;
	}

	public int getGrid_x() {
		return grid_x;
	}

	public void setGrid_x(int grid_x) {
		this.grid_x = grid_x;
	}

	public int getGrid_y() {
		return grid_y;
	}

	public void setGrid_y(int grid_y) {
		this.grid_y = grid_y;
	}

	public long getOwned_timestamp() {
		return owned_timestamp;
	}

	public void setOwned_timestamp(long owned_timestamp) {
		this.owned_timestamp = owned_timestamp;
	}

	public long getOrdered_timestamp() {
		return ordered_timestamp;
	}

	public void setOrdered_timestamp(long ordered_timestamp) {
		this.ordered_timestamp = ordered_timestamp;
	}
}
