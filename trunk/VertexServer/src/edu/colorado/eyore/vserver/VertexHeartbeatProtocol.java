package edu.colorado.eyore.vserver;

import java.util.logging.Logger;
import edu.colorado.eyore.common.net.MessageObjectUtil;
import edu.colorado.eyore.common.net.Protocol;
import edu.colorado.eyore.common.vertex.VertexHeartbeat;
import edu.colorado.eyore.common.vertex.VertexHeartbeatResponse;

/**
 * VertexHeartbeatProtocol class, handles sending of heartbeat messages to the JobServer and
 * responses for starting of any jobs requested.
 */
public class VertexHeartbeatProtocol implements Protocol {

	// Get our logger.
	Logger logger = Logger.getLogger(VertexHeartbeatProtocol.class.getName());
	private VertexHeartbeatResponse vHeartbeatRsp;
	private VertexHeartbeat vHeartbeat;
	private boolean isFinished;
	
	/**
	 * Constructor, initialized the heartbeat and isFinished to false.
	 * @param vHeartbeat
	 */
	public VertexHeartbeatProtocol(VertexHeartbeat vHeartbeat) {
		this.vHeartbeat = vHeartbeat;
		this.isFinished = false;
	}
	
	/**
	 * Returns if the protocol is finished.
	 */
	@Override
	public Boolean isFinished() {
		return isFinished;
	}
	
	/**
	 * Return the protocl object data.
	 */
	@Override
	public Object getData() {
		return this.vHeartbeatRsp;
	}
	
	/**
	 * Handle the protocol between the JobServer.
	 */
	@Override
	public String respondTo(String rcvFromFarEnd) {
		if(rcvFromFarEnd == null) {
			// start by sending a heartbeat to the JobServer.
			
			// Message includes de-codable heartbeat object.
			String vHeartbeatAsString = MessageObjectUtil.objectToString(this.vHeartbeat);
			String resp =  "JSERVER HEARTBEAT:" + vHeartbeatAsString;
			logger.fine("SEND:" + resp);
			return resp;
		} else if (rcvFromFarEnd.startsWith("OK VERTEX:")) {
			// JobServer has Vertices for us to run along with this heartbeat.
			
			// Message includes de-codable heartbeat response object.
			rcvFromFarEnd = rcvFromFarEnd.replace("OK VERTEX:", "");
			this.vHeartbeatRsp = MessageObjectUtil.stringToObject(rcvFromFarEnd);
			this.isFinished = true;
			return null;
		} else {
			// Unknown response from JobServer
			logger.warning("Unknown response from JobServer");
			throw new IllegalStateException("unexpected response from server " + rcvFromFarEnd);
		}
	}
}