package edu.colorado.eyore.common.vertex;

import java.util.List;

/**
 * VertexDescriptor is passed between the VertexServer and the JobServer to track
 * state information about a particular Vertex. Information includes paths to
 * input and output data.
 */
public class VertexDescriptor {
	private String jobId;
	private int vertexNumber;
	private int stageNumber;
	private String vertexClassName;
	private String vertexJar;
	private List<String> inputPaths;
	private VertexOutput output;
	private boolean executionFinished;
	private boolean executionSuccessful;
	private String vertexServerAssignment;
	private Integer numVerticesNextStage;
		
	/**
	 * The ID of the job that this Vertex is a part of
	 */
	public String getJobId(){
		return jobId;
	}
	public void setJobId(String jobId){
		this.jobId = jobId;
	}
	
	/**
	 * The fully qualified class name of this Vertex class
	 * (that will be loaded by the VertexServer)
	 * 
	 * e.g. Of the form "edu.colorado.eyore.SomeClass" - not "SomeClass"
	 * or "edu.colorado.eyore.SomeClass.class".  The later 2 will result
	 * in the class failing to be loaded
	 */
	public String getVertexClassName(){
		return this.vertexClassName;
	}
	public void setVertexClassName(String vertexClassName){
		this.vertexClassName = vertexClassName;
	}


	/**
	 * Get the current Vertex HDFS JAR path for this VertexContext.
	 */
	public String getVertexJarPath() {
		return this.vertexJar;
	}
	public void setVertexJarPath(String jarPath) {
		this.vertexJar = jarPath;
	}
	
	/**
	 * A list of HDFS file paths.  If this is
	 * the first vertex stage, the JobServer will
	 * have examined the input directory for the job
	 * and stored the name of every input file in that
	 * directory in this property.
	 * 
	 */
	public List<String> getInputPaths() {
		return inputPaths;
	}
	public void setInputPaths(List<String> inputPaths) {
		this.inputPaths = inputPaths;
	}

	/**
	 * This allows the JobServer to be informed
	 * of where the Vertex wrote its output
	 * so that the output file paths can be provided
	 * to vertexes in the next stage if necessary
	 * 
	 * On the last stage, the JobServer will
	 * copy any files returned here to the final output
	 * directory (VertexServer doesn't need to know
	 * where this final output directory is located)
	 */
	public VertexOutput getOutput() {
		return output;
	}
	public void setOutput(VertexOutput output) {
		this.output = output;
	}
	
	/**
	 * True when vertex execution is finished - not necessarily
	 * successfully
	 */
	public boolean getExecutionFinished(){
		return this.executionFinished;
	}
	public void setExecutionFinished(boolean executionFinished){
		this.executionFinished = executionFinished;
	}
	
	/**
	 * True if the execution was a success - else
	 * VertexServer should mark it false as having failed 
	 * 
	 */
	public boolean getExecutionSuccessful(){
		return this.executionSuccessful;
	}
	public void setExecutionSuccessful(boolean executionSuccessful){
		this.executionSuccessful = executionSuccessful;
	}
	
	/**
	 * This is the ID of the vertex server that this vertex
	 * has been assigned to for execution (or null if not yet assigned)
	 */
	public String getVertexServerAssignment(){
		return this.vertexServerAssignment;
	}
	public void setVertexServerAssignment(String vserverId){
		this.vertexServerAssignment = vserverId;
	}
	
	/**
	 * This is the zero-based index of the
	 * stage number that this vertex is
	 * a part of 
	 */
	public int getStageNumber(){
		return this.stageNumber;
	}
	public void setStageNumber(int stageNumber){
		this.stageNumber = stageNumber;
	}
	
	/**
	 * This is a zero-based index to identify a vertex within a stage that
	 * may contain any number of vertices 
	 */
	public int getVertexNumber(){
		return this.vertexNumber;
	}
	public void setVertexNumber(int vertexNumber){
		this.vertexNumber = vertexNumber;
	}
	
	/**
	 * Number of vertices in the next stage - necessary
	 * for partitioning output
	 * 
	 * Should be NULL if this vertex is in the 
	 * last stage (this is why Integer is used and not integer)
	 */
	public Integer getNumVerticesNextStage(){
		return numVerticesNextStage;
	}
	public void setNumVerticesNextStage(Integer vertices){
		this.numVerticesNextStage = vertices;
	}
	
	// Needs to be defined so that
	// equality can be compared for objects 
	// when a VertexServer sends a VertexDescriptor
	// and it needs to be compared with one stored
	// by the JobManager (default behavior would
	// compare memory addresses of pointers which is not
	// correct)
	@Override
	public boolean equals(Object o){
		if(this == o){
			return true;
		}
		
		VertexDescriptor other = (VertexDescriptor) o;
		if(other.jobId != this.jobId){
			return false;
		}
		if(other.stageNumber != this.stageNumber){
			return false;
		}
		if(other.vertexNumber != this.vertexNumber){
			return false;
		}
		return true;
	}
	
	// hashcode must be compatible with equals
	@Override
	public int hashCode() {
		return jobId.hashCode() + new Integer(stageNumber + vertexNumber).hashCode();
	}

	
	@Override
	public String toString(){
		return "VertexDescriptor JobID=" + jobId + " STAGE=" + stageNumber + " VERTEX=" + vertexNumber;
	}
}
