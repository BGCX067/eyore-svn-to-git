package edu.colorado.eyore.common.job;

import java.util.List;

import edu.colorado.eyore.common.vertex.VertexStage;

/**
 * This is the class that is implemented by the creator of the job - the actual 
 * computation to run - that specifies the vertex stages, which vertex class is
 * run at each stage and for each stage, how many vertices participate in the 
 * computation
 */
public abstract class JobSpecification {

	protected List<VertexStage> vertexStages;
	protected String inputDataPath;
	protected String outputDataPath;
	
	/**
	 * This is an ordered list of stages in the computation - index 0 is the
	 * the first stage to be executed and N is the last stage
	 */
	public List<VertexStage> getVertexStages(){
		return vertexStages;
	}
	public void setVertexStages(List<VertexStage> vertexStages){
		this.vertexStages = vertexStages;
	}
	
	/**
	 * Set the initial HDFS input directory path for a job. Contains input data files.
	 * @param path Path to input data.
	 */
	public String getInputPath(){
		return inputDataPath;
	}
	public void setInputPath(String path) {
		this.inputDataPath = path;
	}
	
	/**
	 * Set the final HDFS output directory path for a job. Contains output data files.
	 * @param path Path to output data.
	 */
	public String getOutputPath(){
		return this.outputDataPath;
	}
	public void setOutputPath(String path) {
		this.outputDataPath = path;
	}
}
