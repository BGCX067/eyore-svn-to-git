package edu.colorado.eyore.common.vertex;

/**
 * Used by the JobManager to assign a VertexDescriptor (a vertex 
 * to execute) to a VertexServer 
 *
 */
public class VertexServerInfo {
	private String id;
	private int availableThreads;
	
	public void setId(String id) {
		this.id = id;
	}
	public String getId() {
		return id;
	}
	
	/**
	 * This is used by the JobManager to determine
	 * how many vertices to assign to this VServer
	 * when a HeartBeat is received
	 * @return
	 */
	public int getAvailableThreads() {
		return this.availableThreads;
	}
	public void setAvailableThreads(int threads) {
		this.availableThreads = threads;
	}
	
	@Override
	public String toString() {
		return "VertexServerInfo ID=" + id;
	}
}
