package edu.colorado.eyore.common.net;

/**
 * Defines a communication protocol over a socket
 */
public interface Protocol {

	/**
	 * @param rcvFromFarEnd - String received from other end of socket
	 * @return - response to rcvFromFarEnd
	 */
	public String respondTo(String rcvFromFarEnd);
	
	/**
	 * @return true if socket can be closed
	 */
	public Boolean isFinished();

	/**
	 * @return any data that the user of this protocol 
	 * may need to access
	 */
	public Object getData();
}
