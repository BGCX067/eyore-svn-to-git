package edu.colorado.eyore.common.vertex;

import java.util.List;
import java.util.UUID;

/**
 * VertexHeartbeat class contains all required information for a heartbeat message from a
 * VertexServer to the JobServer.
 */
public class VertexHeartbeat {
	private String uid;
	private int numThreads;
	private List<VertexDescriptor> vertexDescriptor;

	/**
	 * Set the VertexServer ID for this heartbeat message.
	 * @param uid ID of the VertexServer sending heartbeat.
	 */
	public void setUid(String uid) {
		this.uid = uid;
	}

	/**
	 * Get the VertexServer ID for this heartbeat message.
	 * @return ID of the VertexServer sending heartbeat.
	 */
	public String getUid() {
		return uid;
	}

	/**
	 * Set the number of threads available for execution in this heartbeat message.
	 * @param numThreads Number of threads available for execution.
	 */
	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
	}

	/**
	 * Get the number of threads available for execution in this heartbeat message.
	 * @return Number of threads available for execution.
	 */
	public int getNumThreads() {
		return numThreads;
	}
	
	public void setVertexDescriptor(List<VertexDescriptor> vertexDescriptor) {
		this.vertexDescriptor = vertexDescriptor;
	}

	public List<VertexDescriptor> getVertexDescriptor() {
		return this.vertexDescriptor;
	}
}
