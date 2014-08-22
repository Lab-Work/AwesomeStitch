package my.osmstitch.mapobjects;


public class ChangeLog extends DBObject {
	
	long timestamp;
	
	long nodes_added;
	long nodes_deleted;
	long nodes_updated;
	long nodes_untouched;

	long links_added;
	long links_deleted;
	long links_updated;
	long links_untouched;

	String file_name;
	String description;

	public ChangeLog(long timestamp, String fileName, String description){
		this.timestamp = timestamp;
		this.file_name = fileName;
		this.description = description;
	}
	
	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return "tmp_schema.changelog";
	}

	@Override
	public void setId(long id) {
		//do nothing - we have no ids, only timestamps
	}

	@Override
	public String getInsertStatement() {
		String sql = "INSERT INTO " + getTableName() + " (timestamp, nodes_added, nodes_deleted, nodes_updated, nodes_untouched, links_added, "+
				"links_deleted, links_updated, links_untouched, file_name, description) VALUES(" + timestamp + ", " + nodes_added + ", " + nodes_deleted + 
				", " + nodes_updated + ", " + nodes_untouched + ", " + links_added + ", " + links_deleted + ", " + links_updated + ", " +
				links_untouched + ", '" + file_name +  "', '" + description + "');";
		return sql;
	}

	@Override
	public String getHighestIdQuery() {
		// TODO Auto-generated method stub
		return "SELECT 0;";
	}

	@Override
	public String getCSVLine() {
		// TODO Auto-generated method stub
		String csv = timestamp + "|" + nodes_added + "|" + nodes_deleted + "|" + nodes_updated + "|" + nodes_untouched + "|" + links_added + "|" + 
		links_deleted + "|" + links_updated + "|" + links_untouched + "|" + file_name + "|" + description;
		return csv;
	}

	@Override
	public int getServerBufferSize() {
		return 10;
	}
	
	
	
	
	public String toString(){
		return "Added " + nodes_added + " nodes. Deleted " + nodes_deleted + " nodes. Updated " + nodes_updated + " nodes. Left alone " + nodes_untouched + " nodes. " +
				"Added " + links_added + " links. Deleted " + links_deleted + " links. Updated " + links_updated + " links. Left alone " + links_untouched + " links. ";
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getNodes_added() {
		return nodes_added;
	}

	public void setNodes_added(long nodes_added) {
		this.nodes_added = nodes_added;
	}

	public long getNodes_deleted() {
		return nodes_deleted;
	}

	public void setNodes_deleted(long nodes_deleted) {
		this.nodes_deleted = nodes_deleted;
	}

	public long getNodes_updated() {
		return nodes_updated;
	}

	public void setNodes_updated(long nodes_updated) {
		this.nodes_updated = nodes_updated;
	}

	public long getNodes_untouched() {
		return nodes_untouched;
	}

	public void setNodes_untouched(long nodes_untouched) {
		this.nodes_untouched = nodes_untouched;
	}

	public long getLinks_added() {
		return links_added;
	}

	public void setLinks_added(long links_added) {
		this.links_added = links_added;
	}

	public long getLinks_deleted() {
		return links_deleted;
	}

	public void setLinks_deleted(long links_deleted) {
		this.links_deleted = links_deleted;
	}

	public long getLinks_updated() {
		return links_updated;
	}

	public void setLinks_updated(long links_updated) {
		this.links_updated = links_updated;
	}

	public long getLinks_untouched() {
		return links_untouched;
	}

	public void setLinks_untouched(long links_untouched) {
		this.links_untouched = links_untouched;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	

	public void incrementNodes_added(){
		nodes_added++;
	}

	public void incrementNodes_deleted(){
		nodes_deleted++;
	}

	public void incrementNodes_updated(){
		nodes_updated++;
	}

	public void incrementNodes_untouched(){
		nodes_untouched++;
	}


	public void incrementLinks_added(){
		links_added++;
	}

	public void incrementLinks_deleted(){
		links_deleted++;
	}

	public void incrementLinks_updated(){
		links_updated++;
	}

	public void incrementLinks_untouched(){
		links_untouched++;
	}
}
