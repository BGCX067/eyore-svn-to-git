package edu.colorado.eyore.common.vertex;

import java.util.List;

/**
 * VertexHeartbeatResponse class, maintains a list of Vertices to respond with to the VertexServer
 * following a heartbeat to the JobServer.
 */
public class VertexHeartbeatResponse {
	private List<VertexDescriptor> vertexDescriptor;

	public void setVertexDescriptor(List<VertexDescriptor> vertexDescriptor) {
		this.vertexDescriptor = vertexDescriptor;
	}

	public List<VertexDescriptor> getVertexDescriptor() {
		return this.vertexDescriptor;
	}
}
