package edu.colorado.eyore.common.net;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;


/**
 * Socket messages may contain objects represented as Strings.  This class
 * allows converting String -> Object and Object -> String.
 */
public class MessageObjectUtil {

	/**
	 * Convert an object to a string to be sent as a message over a socket
	 * 
	 * @param o - any object BUT IT MUST BE A JAVA BEAN (zero-arg constuctor,
	 * properties are private variables with public getter and setter methods)
	 * 
	 * @return - String (xml representation of the object that can be decoded)
	 */
	public static String objectToString(Object o){
		ByteArrayOutputStream jobDescAsBytes = new ByteArrayOutputStream();
		XMLEncoder encoder = new XMLEncoder(jobDescAsBytes);
		encoder.writeObject(o);
		encoder.close();
		
		// keeping all messages on one line
		return jobDescAsBytes.toString().replaceAll("\\n", " ");
	}
	
	/**
	 * Convert a string previously encoded by objectToString into that object
	 * 
	 * @param <T>
	 * @param string - the xml representation of the output produced by objectToString
	 * @return - the decoded object instance
	 */
	public static <T> T stringToObject(String string){
		// Get the job descriptor by converting from XML to the JobDescriptor object
		XMLDecoder decodeDescriptor = new XMLDecoder(new ByteArrayInputStream(string.getBytes()));
		return (T) decodeDescriptor.readObject();	
	}
}
