package edu.colorado.eyore.common.vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Used to track where a Vertex wrote its output.  This has to be tracked
 * because individual output files of a vertex will be intended
 * for execution on specific vertices in the next vertex stage
 *
 */
public class VertexOutput {

	private Map<Integer, List<String>> outputMap = new HashMap<Integer, List<String>>();
	
	/**
	 * Output map is a Map with entries of the form 
	 * (index of vertex in next stage OR NULL if this vertex is in the last stage, 
	 * 	list of paths to output files on HDFS
	 *  written by this vertex upon successful completion)
	 * 
	 * - Probably want to implement this as 
	 *   java.util.HashMap<Integer, java.util.ArrayList<String>> because HashMap definitely
	 *   supports NULL as a key value and ArrayList is easy to use
	 * 
	 */
	public Map<Integer, List<String>> getOutputMap(){
		return outputMap;
	}
	public void setOutputMap(Map<Integer,List<String>>outputMap){
		this.outputMap = outputMap;
	}
	

}
