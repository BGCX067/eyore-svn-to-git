package edu.colorado.eyore.jserver;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.colorado.eyore.common.hdfs.HdfsUtils;
import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.job.JobStatus;
import edu.colorado.eyore.common.vertex.VertexDescriptor;
import edu.colorado.eyore.common.vertex.VertexOutput;
import edu.colorado.eyore.common.vertex.VertexServerInfo;
import edu.colorado.eyore.common.vertex.VertexStage;

/**
 * The Job server scheduling/status updating/allocating vertex execution
 * will probably get hairy, so making a separate class to handle this stuff
 * 
 * This class must be thread safe & nothing in the JobStatus instances
 * should be changed outside of this class
 * 
 * Updates to job status/allocation/etc are event driven - request from
 * Client or VertexServer triggers any updating
 *
 */
public class JobManager {
	
	private static Logger logger = Logger.getLogger(JobManager.class.getName());
	
	/**
	 * A queue for new jobs sent from a client that have not yet been
	 * processed  
	 */
	protected ArrayDeque<JobDescriptor> unprocessedJobQ = new ArrayDeque<JobDescriptor>();
	
	/**
	 * Stores jobs that are in progress (and any that have finished, but have not been purged
	 * because the Client status request hasn't happened yet)
	 */
	protected HashMap<String, JobDescriptor> jobsInProgress = new HashMap<String, JobDescriptor>();
	
	/**
	 * These are vertices that are available to be assigned to vertex servers
	 */
	protected ArrayDeque<VertexDescriptor> allocatableVertices = new ArrayDeque<VertexDescriptor>();
	
	/**
	 * These are vertices that have ALREADY been assigned to a vertex server & we are waiting on
	 * a success/failure update about each one from the assigned VertexServer 
	 */
	protected HashSet<VertexDescriptor> verticesInProgress = new HashSet<VertexDescriptor>();
	
	/**
	 * Tracks output from vertices needed by vertices in next stage
	 * 
	 * Maps JobID -> (Maps Vertex Stage Number -> (Maps Vertex Number -> Output File Path ))
	 * 
	 */
	protected HashMap<String, HashMap<Integer, HashMap<Integer,ArrayList<String>>>> outputMap =  
		new HashMap<String, HashMap<Integer,HashMap<Integer,ArrayList<String>>>>();
		
	protected int numSimultenousJobs;
	
	private HdfsUtils hdfs;
	
	private int fileSplitsPerHdfsBlock;
	
	/**
	 * 
	 * @param numSimultaneousJobs - the number of simultaneously executing jobs
	 * supported - any new jobs added when this number is currently executing
	 * will be held in a queue until one or more currently running jobs is completed
	 */
	public JobManager(int numSimultaneousJobs, HdfsUtils hdfs, int fileSplitsPerHdfsBlock){
		this.numSimultenousJobs = numSimultaneousJobs;
		this.hdfs = hdfs;
		this.fileSplitsPerHdfsBlock = fileSplitsPerHdfsBlock;
		logger.info("JobManager initialized");
	}
	
	/**
	 * A job arrives from the client is added into the manager via this
	 * method
	 * 
	 */
	public synchronized void addNewJobFromClient(JobDescriptor jobDescriptor){
		unprocessedJobQ.add(jobDescriptor);
		logger.info("Enqueued job " + jobDescriptor);
	
		// For the first vertex stage of newly added jobs, this
		// will take care of making sure their vertices
		// are allocatable
		updateAllocatableVertices();
	}
	
	/**
	 * Called when a vertex server heartbeat indicates
	 * it can execute more vertices.  The returned
	 * VertexDescriptors indicate which vertices the
	 * VertexServer has been newly assigned.
	 * 
	 * @param vserver - VertexServerInfo that identifies a VertexServer (the one that
	 * just made the heartbeat request)
	 * 
	 * @return A list of VertexDescriptor that represents
	 * the vertices newly assigned to this server - assignment
	 * is done based on the capacity of this server
	 */
	public synchronized List<VertexDescriptor> assignVerticesToServer(VertexServerInfo vserver){
		ArrayList<VertexDescriptor> assignedVertices = new ArrayList<VertexDescriptor>();
		
		if(vserver.getAvailableThreads() == 0){
			logger.warning("vertex server assign called with no available threads");
			return assignedVertices;
		}
		
		
		// For now just assign the next available vertices up to available/3
		// - may need to tune later
		int maxToAssign = (int)Math.ceil(vserver.getAvailableThreads()/3.0);
		for(int i = 0; i < maxToAssign; i++){
			if(allocatableVertices.size() == 0){
				break;
			}
			VertexDescriptor vertex = allocatableVertices.remove();
			vertex.setVertexServerAssignment(vserver.getId());
			verticesInProgress.add(vertex);
			assignedVertices.add(vertex);
			
			logger.info("Assigned Vertex (" + vertex + ") to VServer (" + vserver + ")");
		}
		return assignedVertices;
	}
	
	
	
	/**
	 * Called when a HeartBeat from the VertexServer indicates
	 * that a Vertex has finished executing and it failed or
	 * suceeded.  For now, any such failure will result in the whole
	 * job being marked as failed.
	 * 
	 * - Should only be called for vertices that have been completely
	 * executed (finished succesfully or else finished to to failure)
	 * 
	 */
	public synchronized void updateVertexStatus(VertexDescriptor vertex){
		
		// make sure job still exists (could be status report after job
		// already failed)
		JobDescriptor job = jobsInProgress.get(vertex.getJobId());
		if(job == null){
			// job no longer exists
			return;
		}
		JobStatus status = job.getJobStatus();
		
		// in case of future enhancement where duplicate vertex execution
		// can happen, make sure that any status reported on previous
		// stages of the job are ignored
		if(vertex.getStageNumber() < status.getCurrentVertexStage()){
			logger.info("Rejecting status update of vertex because its less than current job stage: " +
					vertex);
			return;
		}
		
		// This shouldnt happen - but if it does, 
		// don't do anything
		if(! vertex.getExecutionFinished()){
			// this actually could be used as a mechanism to track that a vertex
			// isn't suck forever & needs to be timed out, but for now, not
			// expecting status to be reported until a vertex is finished
			logger.warning("Vertex Server (" + vertex.getVertexServerAssignment()+ ") reported status for still executing vertex " + vertex);
			return;
		}
		
		logger.info("Status update from V. Server (" + 
				vertex.getVertexServerAssignment() + ") Vertex (" + vertex + ") " +
						"completed with " + (vertex.getExecutionSuccessful() ? "Success" : "FAILURE"));
		
		// Right now, whole job fails if
		// single vertex fails
		if(! vertex.getExecutionSuccessful()){
			failJob(vertex);
			return;
		}				

		status.setTotalVerticesInProgressCurrentStage(
				status.getTotalVerticesInProgressCurrentStage()-1);
		
		// Remove the vertex from the assigned vertices set
		verticesInProgress.remove(vertex);
		
		Integer nextStageNumber = null;
		if(vertex.getStageNumber()+1 < job.getJobStatus().getTotalVertexStages()){
			// If there is a next stage, record its index
			nextStageNumber = vertex.getStageNumber() +1;
		}
		
		// record the output of this vertex
		HashMap<Integer, ArrayList<String>> stageOutputMap = outputMap.get(job.getJobId()).get(nextStageNumber);
		if(stageOutputMap == null){
			stageOutputMap = new HashMap<Integer,ArrayList<String>>();
			outputMap.get(job.getJobId()).put(nextStageNumber, stageOutputMap);			
		}
		VertexOutput vOut = vertex.getOutput();
		if(vOut == null || vOut.getOutputMap() == null){
			logger.warning("Vertex " + vertex + " did not have a non-null output map");
		}else{
			for(Integer nextVertex : vOut.getOutputMap().keySet()){
				if(vOut.getOutputMap().get(nextVertex) != null){
					List<String> outputFiles = vOut.getOutputMap().get(nextVertex);
					for(String file : outputFiles){
						if(! stageOutputMap.containsKey(nextVertex)){
							stageOutputMap.put(nextVertex, new ArrayList<String>());
						}
						stageOutputMap.get(nextVertex).add(file);
					}
				}else{
					logger.warning("Vertex " + vertex + " had null output list for next vertex number " + 
							nextVertex);
				}
			}
		}
		
		
		// Makes sure that any ready to run vertices
		// are allocatable
		updateAllocatableVertices();
	}
	
	/**
	 * This is used for handling status requests from a Client;
	 * the JobServer will send this back to the Client & the
	 * Client can interpret the status
	 * 
	 * If the queried job is finished executing it is removed
	 * from the "jobs in progress" data structure
	 * 
	 * @param jobId
	 * @return
	 */
	public synchronized JobStatus jobStatusQuery(String jobId){
		JobDescriptor job = jobsInProgress.get(jobId);
				
		// no status available
		if(job == null){
			logger.info("Status query for jobID=" + jobId + " - JOB NOT NOT FOUND");
			return null;
		}

		JobStatus status = job.getJobStatus();		
		
		// purge the job if its finished
		if(status.getExecutionFinished()){
			logger.info("Status query for jobID=" + jobId + " - JOB FINISHED");
			jobsInProgress.remove(jobId);
		}else{
			logger.info("Status query for jobID=" + jobId + " - JOB IN PROGRESS");
		}
		return status;
	}
	
	/**
	 * Marks the job as failed and does necessary cleanup - however,
	 * job stays in jobsInProgress queue until getJobStatus is called
	 * so that client knows how/why job failed before its record is deleted
	 * 
	 * @param jobId
	 */
	private void failJob(String jobId){
		JobDescriptor job = jobsInProgress.get(jobId);
		
		if(job==null){
			// job could have already been failed by another thread &
			// already removed by a client status query
			logger.info("Failing JOB ID=" + jobId + " - job not found");			
		}else{
			logger.info("Failing JOB ID=" + jobId + " " + job);
		}
		
		job.getJobStatus().setExecutionFinished(true);
		// note: job is not removed from queue until next client
		// status request so that we can report more info to
		// the client on that request
		
		// Remove vertices for the failed job
		Iterator<VertexDescriptor> itr = allocatableVertices.iterator();
		while(itr.hasNext()){
			if(itr.next().getJobId().equals(jobId)){
				itr.remove();
			}
		}
		
		// Remove vertices for the failed job
		itr = verticesInProgress.iterator();
		while(itr.hasNext()){
			if(itr.next().getJobId().equals(jobId)){
				itr.remove();
			}
		}
		
		// wack the output map
		outputMap.remove(jobId);
	}
	
	/**
	 * Job is failed as a result of a failed vertex
	 * 
	 * @param vertex
	 */
	private void failJob(VertexDescriptor vertex){
		String jobId = vertex.getJobId();
		failJob(jobId);
		
	}
	
	/**
	 * Puts job in the map of currently running jobs & initializes its JobStatus 
	 */
	private void startJob(JobDescriptor job){
		JobStatus jobStatus = new JobStatus();
		job.setJobStatus(jobStatus);
		
		// indicates to allocation function
		// that the first stage needs to have
		// its vertices made allocatable
		jobStatus.setCurrentVertexStage(-1);
		jobStatus.setTotalVertexStages(job.getJobSpecification().getVertexStages().size());
		
		jobStatus.setExecutionFinished(false);
		jobsInProgress.put(job.getJobId(), job);
		
		// setup the output map for this job
		outputMap.put(job.getJobId(), new HashMap<Integer, HashMap<Integer,ArrayList<String>>>());
		
		if(jobStatus.getTotalVertexStages() == 0){
			// Fail job
			logger.severe("Job has no vertex stages");
			jobStatus.setExecutionFinished(true);
			return;
		}
		
		logger.info("Started Job " + job);
	}
	
	/**
	 * - If any space available in jobsInProgress queue,
	 * enqueues jobs from the unprocessed jobs queue
	 * - Iterates over jobs in progress, making sure that
	 * any allocatable Vertex's are allocatable
	 */
	private void updateAllocatableVertices(){
		
		// If less than the max number simultaneous jobs are running,
		// remove jobs from the unprocessed Q and start them
		while(unprocessedJobQ.size() > 0 && jobsInProgress.size() < numSimultenousJobs){
			startJob(unprocessedJobQ.remove());			
		}
		
		// Now make sure all allocatable vertices are allocatable
		jobLoop: for(JobDescriptor job : jobsInProgress.values()){			
			JobStatus jobStatus  = job.getJobStatus();
			if(jobStatus.getExecutionFinished()){
				continue;
			}
			
			boolean newStageToUpdate = false;
			if(jobStatus.getCurrentVertexStage() < 0){
				// new job..need to start first stage
				newStageToUpdate = true;
				jobStatus.setCurrentVertexStage(0);
			}else{
				if(jobStatus.getTotalVerticesInProgressCurrentStage() == 0){
					// Current vertex stage is completed
					
					if((jobStatus.getCurrentVertexStage()+1) == jobStatus.getTotalVertexStages()){
						// this job is completely finished
						jobStatus.setExecutionFinished(true);

						ArrayList<String> hdfsOutputFilePaths = new ArrayList<String>();
						
						// get list of output file paths on HDFS
						HashMap<Integer, ArrayList<String>> lastStageOutput = outputMap.get(job.getJobId()).get(null);
						for(ArrayList<String> vertexOutFiles : lastStageOutput.values()){
							if(vertexOutFiles == null || vertexOutFiles.isEmpty()){
								continue;
							}
							for(String filePathStr : vertexOutFiles){
								hdfsOutputFilePaths.add(filePathStr);
							}
						}
						
						// rename output files on HDFS so that they are in the correct final directory
						// as specified in the job specification
						try{
							if(hdfsOutputFilePaths.size() > 0){
								hdfs.moveFilesToNewDir(hdfsOutputFilePaths, job.getJobSpecification().getOutputPath());
							}else{
								logger.warning("JOB HAD NO OUTPUT FILES! NOT CREATING FINAL OUTPUT DIRECTORY");
							}
						}catch(Exception e){
							logger.log(Level.SEVERE, "FAILED 'moving' output on HDFS from last stage into final output directory " +
									job.getJobSpecification().getOutputPath(), e);
						}
						
						outputMap.remove(job.getJobId());
						continue;
					}else{
						// this job needs to run its next stage
						jobStatus.setCurrentVertexStage(jobStatus.getCurrentVertexStage()+1);
						newStageToUpdate = true;
					}
				}
				// else current vertex stage is still in progress
			}
			
			// If there's a new Vertex stage now, make all Vertex's in the stage
			// allocatable
			if(newStageToUpdate){
				int stageIndex = jobStatus.getCurrentVertexStage();
				VertexStage stage = job.getJobSpecification().getVertexStages().get(stageIndex);
			
				
				// List of input files in case this is the first stage
				List<String> files = null;
				
				if(stageIndex == 0){
					String inputDir = job.getJobSpecification().getInputPath();
					
					try{
						files = hdfs.getFilePathsFromHdfsDir(inputDir, fileSplitsPerHdfsBlock);
					}catch(IOException e){
						logger.log(Level.SEVERE, "Failed to get input files from dir " + 
								inputDir, e);
						failJob(job.getJobId());
						continue jobLoop;
					}
					if(files.size() == 0){
						logger.warning("Did not find any input files in dir " + 
								inputDir);
					}						

					// set first stage vertex count to 
					// number of HDFS files
					stage.setNumVertices(files.size());

				}
				jobStatus.setTotalVerticesCurrentStage(stage.getNumVertices());
				jobStatus.setTotalVerticesInProgressCurrentStage(stage.getNumVertices());	
				
								
				for(int vertexIndex = 0; vertexIndex < stage.getNumVertices(); vertexIndex++){
					VertexDescriptor newV = new VertexDescriptor();
					newV.setExecutionFinished(false);

					if(stageIndex == 0){
						List<String> singleFile = new ArrayList<String>();
						singleFile.add(files.get(vertexIndex));
						newV.setInputPaths(singleFile);
					}else{
						// need to get input files from previous stage
						HashMap<Integer, ArrayList<String>> stageOutputMap = 
							outputMap.get(job.getJobId()).get(stageIndex);
						ArrayList<String> inputFiles = stageOutputMap.get(vertexIndex);
						if(inputFiles == null){
							inputFiles = new ArrayList<String>();
						}
						newV.setInputPaths(inputFiles);
					}
					
					newV.setJobId(job.getJobId());
					newV.setVertexClassName(stage.getVertex().getName());
					newV.setVertexJarPath(job.getHdfsJarPath());
					newV.setStageNumber(stageIndex);
					newV.setVertexNumber(vertexIndex);
					newV.setVertexServerAssignment(null);
					
					// determine and set number of vertices in next stage
					if((stageIndex + 1) == jobStatus.getTotalVertexStages()){
						// vertex is part of last stage
						newV.setNumVerticesNextStage(null);	
					}else{					
						int vNextStage = job.getJobSpecification().getVertexStages().get(stageIndex+1).getNumVertices();
						newV.setNumVerticesNextStage(vNextStage);
					}

					allocatableVertices.add(newV);
				}
			}
		}
	}
	
	
}
