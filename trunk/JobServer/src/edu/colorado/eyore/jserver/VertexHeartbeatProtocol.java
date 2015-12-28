package edu.colorado.eyore.jserver;

import java.util.logging.Logger;

import edu.colorado.eyore.common.net.MessageObjectUtil;
import edu.colorado.eyore.common.net.Protocol;
import edu.colorado.eyore.common.vertex.VertexHeartbeat;
import edu.colorado.eyore.common.vertex.VertexHeartbeatResponse;
import edu.colorado.eyore.common.vertex.VertexServerInfo;

/**
 * JobServer VertexHeartbeatProtocl. Handles heartbeat messages from the VertexServer.
 */
public class VertexHeartbeatProtocol implements Protocol {
	// Get our logger.
	Logger logger = Logger.getLogger(VertexHeartbeatProtocol.class.getName());

	// Private member variables (such as heartbeat and if protocol is finished).
	private VertexHeartbeat vHeartbeat;
	private boolean isFinished;
	
	private JobManager jobManager;
	
	/**
	 * Constructor, initialized isFinished to false.
	 * @param jobManager 
	 */
	public VertexHeartbeatProtocol(JobManager jobManager) {
		this.isFinished = false;
		this.jobManager = jobManager;
	}
	
	/**
	 * Get the object data for this protocol (type VertexHeartBeat).
	 */
	@Override
	public Object getData() {
		return this.vHeartbeat;
	}

	/**
	 * Return if this protocol is finished.
	 */
	@Override
	public Boolean isFinished() {
		return this.isFinished;
	}

	/**
	 * Protocol response to a received heartbeat message.
	 */
	@Override
	public String respondTo(String rcvFromFarEnd) {
		// Handle the heartbeat from the JobServer.
		if(rcvFromFarEnd.startsWith("JSERVER HEARTBEAT:")){
			this.vHeartbeat = MessageObjectUtil.stringToObject(
					rcvFromFarEnd.replaceFirst("JSERVER HEARTBEAT:", ""));
			
			VertexServerInfo vServer = new VertexServerInfo();
			vServer.setId(vHeartbeat.getUid());
			vServer.setAvailableThreads(vHeartbeat.getNumThreads());
			VertexHeartbeatResponse vhbr = new VertexHeartbeatResponse();
			vhbr.setVertexDescriptor(this.jobManager.assignVerticesToServer(vServer));
			
			this.isFinished = true;
			logger.info("HEARTBEAT numVertices:" + vhbr.getVertexDescriptor().size());
			return "OK VERTEX:" +
				MessageObjectUtil.objectToString(vhbr);
		}

		return null;
	}
}
