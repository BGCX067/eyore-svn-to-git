package edu.colorado.eyore;

import java.util.ArrayList;

import edu.colorado.eyore.common.job.JobSpecification;
import edu.colorado.eyore.common.vertex.VertexStage;

/**
 * This is an example job that reads some input and 
 * echos it as output
 */
public class EchoJob extends JobSpecification {
	
	// Must have a zero-argument constructor
	public EchoJob(){
		super.vertexStages = new ArrayList<VertexStage>();
		
		// note: input/output job directory HDFS paths are specified on client.sh command line
		
		// define the first stage & add to stage list
		// - JobServer will load this class, instantiate it
		//   and invoke the getVertexStages() (defined in
		//   the super-class) to get the list of stages
		VertexStage echoStage = new VertexStage();
		echoStage.setVertex(EchoVertex.class);
		vertexStages.add(echoStage);
	}
	
	
}
