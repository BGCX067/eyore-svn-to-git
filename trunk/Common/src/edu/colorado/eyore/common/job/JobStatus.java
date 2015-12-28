package edu.colorado.eyore.common.job;

/**
 * Maintains the status of the current job - used to
 * know when to allocate vertexes, to know when
 * a Job is finsihed and can be used to
 * report status to the Client
 *
 */
public class JobStatus {
	
	private int currentVertexStage;	
	private int totalVertexStages;
	private int totalVerticesCurrentStage;	
	private int totalVerticesInProgressCurrentStage;	
	private boolean executionFinished;
	
	/**
	 * The zero-based index of the vertex
	 * stage currently in progress
	 */
	public int getCurrentVertexStage(){
		return currentVertexStage;
	}
	public void setCurrentVertexStage(int stageNumber){
		this.currentVertexStage = stageNumber;
	}
	
	/**
	 * How many stages exist in the job
	 */
	public int getTotalVertexStages(){
		return totalVertexStages;
	}
	public void setTotalVertexStages(int totalStages){
		this.totalVertexStages = totalStages;
	}
	
	/**
	 * How many vertices in the current stage total 
	 */
	public int getTotalVerticesCurrentStage(){
		return totalVerticesCurrentStage;
	}
	public void setTotalVerticesCurrentStage(int vertices){
		this.totalVerticesCurrentStage = vertices;
	}
	
	/**
	 * How many vertices in the current stage are not finished
	 */
	public int getTotalVerticesInProgressCurrentStage(){
		return totalVerticesInProgressCurrentStage;
	}
	public void setTotalVerticesInProgressCurrentStage(int vertices){
		this.totalVerticesInProgressCurrentStage = vertices;
	}
	

	/**
	 * This may be true without all vertex stages and/or vertices
	 * being executed in case of failure 
	 * 
	 * When true, the JobManager will discard this job after 
	 * the next Client status request for this job (so that
	 * the client sees that it failed)
	 * 
	 */
	public boolean getExecutionFinished(){
		return executionFinished;
	}
	public void setExecutionFinished(boolean executionFinished){
		this.executionFinished = executionFinished;
	}
	
}
