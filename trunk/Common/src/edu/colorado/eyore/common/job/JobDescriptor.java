package edu.colorado.eyore.common.job;


public class JobDescriptor {

	private String jobId;
	public String getJobId(){
		return jobId;
	}
	public void setJobId(String jobId){
		this.jobId = jobId;
	}
	
	private String hdfsJarPath;
	public String getHdfsJarPath(){
		return hdfsJarPath;
	}
	public void setHdfsJarPath(String hdfsJarPath){
		this.hdfsJarPath = hdfsJarPath;
	}

	/**
	 * The path specified on the client program command line
	 * for the HDFS input directory of the job - job server
	 * will pass this to the JobSpecification
	 */
	public String getHdfsJobInputDir(){
		return hdfsJobInputDir;
	}
	public void setHdfsJobInputDir(String inputDir){
		this.hdfsJobInputDir = inputDir;
	}
	private String hdfsJobInputDir;
	
	/**
	 * Path specified for final HDFS output dir 
	 */
	public String getHdfsJobOutputDir(){
		return hdfsJobOutputDir;
	}
	public void setHdfsJobOutputDir(String outputDir){
		this.hdfsJobOutputDir = outputDir;
	}
	private String hdfsJobOutputDir;
	
	
	private JobSpecification jobSpecification;
	public JobSpecification getJobSpecification(){
		return jobSpecification;
	}
	public void setJobSpecification(JobSpecification jobSpecification){
		this.jobSpecification = jobSpecification;
	}
	
	private JobStatus jobStatus;
	public JobStatus getJobStatus(){
		return this.jobStatus;
	}
	public void setJobStatus(JobStatus jobStatus){
		this.jobStatus = jobStatus;
	}
	
	@Override
	public String toString(){
		return "JobDescriptor ID=" + jobId + " Jar=" + hdfsJarPath;
	}
}
