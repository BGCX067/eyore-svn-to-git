package edu.colorado.eyore.client;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import edu.colorado.eyore.common.hdfs.HdfsUtils;
import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.job.JobStatus;
import edu.colorado.eyore.common.net.RequestResponseUtil;

public class Client {

	protected static Logger logger = Logger.getLogger(Client.class.getName());

	protected File jobJarFile;
	protected String jserverHost;
	protected int jserverPort;
	protected String namenodeHost;
	protected int namenodePort;
	protected HdfsUtils hdfs;
	protected String hdfsInputDirPath;
	protected String hdfsOutputDirPath;

	/**
	 * ID for this job from the server
	 */
	protected String jobId;

	public Client(File jobJarFile, String hdfsInputDirPath, 
			String hdfsOutputPath, Properties props) {
		this.jobJarFile = jobJarFile;
		this.hdfsInputDirPath = hdfsInputDirPath;
		this.hdfsOutputDirPath = hdfsOutputPath;
		this.jserverHost = props.getProperty("jobserver.address");
		this.jserverPort = Integer
				.parseInt(props.getProperty("jobserver.port"));
		this.namenodeHost = props.getProperty("namenode.address");
		this.namenodePort = Integer
				.parseInt(props.getProperty("namenode.port"));

		try {
			hdfs = new HdfsUtils(namenodeHost, namenodePort);
		} catch (IOException e) {
			throw new RuntimeException("Failed to access HDFS", e);
		}
	}

	/**
	 * Starts the client
	 * 
	 * @return exit code (0 success else non-zero)
	 */
	public int start() throws IOException {

		// client sends request and gets response with job ID
		JobDescriptor jDesc = (JobDescriptor) RequestResponseUtil.
			clientRequest(new RequestJobProtocol(), jserverHost, jserverPort);
		
		// Validate HDFS job input directory to avoid frustration later
		// when job fails as a result
		boolean directoryExists = false;
		try{
			directoryExists = hdfs.hdfsDirExists(hdfsInputDirPath);			
		}catch(Exception e){
			logger.log(Level.SEVERE, "Exception on validation of HDFS job input dir.  " +
					"DOES IT EXIST???: " +
					hdfsInputDirPath, e);
			return 1;
		}
		if(! directoryExists){
			logger.severe("HDFS input directory does not exist: " +
					hdfsInputDirPath);
			return 1;
		}
		
		// write jar to HDFS & then tell server
		// to start the job
		logger.info("Received JOB ID " + jDesc.getJobId());
		String remoteJarPath = "/jobs/" + jobJarFile.getName() + "_ID"
				+ jDesc.getJobId();
		try {
			hdfs.writeLocalFileToHdfs(jobJarFile, remoteJarPath);
			logger.info("Wrote jar to HDFS @ " + remoteJarPath);
		} catch (IOException e) {
			throw new RuntimeException("failed writing jar to HDFS", e);
		}

		// Tell server about jar & ask server to start job (in one message)
		jDesc.setHdfsJobInputDir(hdfsInputDirPath);
		jDesc.setHdfsJobOutputDir(hdfsOutputDirPath);
		jDesc.setHdfsJarPath(remoteJarPath);
				
		boolean jobStarted = (Boolean)RequestResponseUtil.clientRequest(
				new StartJobProtocol(jDesc),
				jserverHost, jserverPort);
		if(jobStarted){
			logger.info("Job started successfully");
		}else{
			logger.severe("Job FAILED to start");
			return 1; // failure
		}
		
		Date startTime = new Date();
		logger.info("Job started at " + startTime); 
		
		// Now poll for status until job is completed
		JobStatus jobStatus = null;
		boolean jobInProgress = true;
		while(jobInProgress){

			try{
				jobStatus = (JobStatus)RequestResponseUtil.clientRequest(
					new JobStatusRequestProtocol(jDesc),jserverHost, jserverPort);
			}catch(Exception e){
				logger.log(Level.SEVERE, "Job status request with exception", e);
				return 1;
			}
			
			try{
				Thread.sleep(5000);
			}catch(InterruptedException e){
				
			}
			if(jobStatus != null){
				if(jobStatus.getExecutionFinished()){
					jobInProgress = false;
					break;
				}else{
					printJobSummary(jobStatus);
				}
			}else{
				logger.severe("Job Server did not have record of the job");
				return 1;
			}			
		}
		
		int ret =  printJobSummary(jobStatus);
		logger.info("Note: job was started at " + new Date());
		return ret;
	}

	private int printJobSummary(JobStatus jobStatus) {
		int lastStage = jobStatus.getCurrentVertexStage();
		int totalStages = jobStatus.getTotalVertexStages();
		int totalLastStageVs = jobStatus.getTotalVerticesCurrentStage();
		int totalLastStageNotFinished = jobStatus.getTotalVerticesInProgressCurrentStage();
		
		boolean completed = (lastStage == (totalStages-1)) && (totalLastStageNotFinished == 0);
		if(jobStatus.getExecutionFinished()){			
			if(completed){
				logger.info("**** JOB COMPLETED EXECUTING SUCESSFULLY ****");
			}else{
				logger.severe("!!!!! JOB DID NOT COMPLETE ALL STAGES & VERTICES !!!!");
			}
			logger.info("Finished at " + new Date() );
		}
		
		logger.info(
				
				"Total stages in job: " + totalStages + "\n" +
				"Stages completed executing: " + (lastStage + (completed ? 1 : 0)) + "\n" + 
				"Number of vertices in last stage executed: " + totalLastStageVs + "\n" +
				"Number of vertices in last stage did not finish: " + totalLastStageNotFinished + "\n");
		
		
		
		return (completed ? 0 : 1);
	}

	private static void printUsageAndExit(){
		System.err.println("Usage: client.sh <local job jar path> <HDFS input path> <HDFS output path>");
		System.exit(1);
	}
	
	/**
	 * @param args
	 *            [0] path to jar for job 
	 *            [1] path to HDFS input dir
	 *            [2] path to properties file 
	 *            [3] path to logging configuration file
	 */
	public static void main(String[] args) throws IOException {

		// Get the path to the jar file for the job REQUIRED
		if (args.length == 0 || args[0] == null || args[0].isEmpty()) {
			logger.severe("1st arg: local file system path to jar file is required");
			System.exit(1);
		}
		File jarFile = new File(args[0]);
		if (!jarFile.exists()) {
			logger.severe("Can't find jar file: " + jarFile.getAbsolutePath());
			printUsageAndExit();
		}
		
		// Get path to HDFS input directory - REQUIRED
		if(args.length < 2 || args[1] == null || args[1].isEmpty()){
			logger.severe("2nd arg: HDFS input directory path is required");
			printUsageAndExit();
		}
		final String hdfsInputDirPath = args[1];
		
		// Get path to HDFS output directory - REQUIRED
		if(args.length < 3 || args[2] == null || args[2].isEmpty()){
			logger.severe("3nd arg: HDFS output directory path is required");
			printUsageAndExit();
		}
		final String hdfsOutputDirPath = args[2];		
	
		// get path to properties file - default to default properties file in
		// current dir if not specified
		File propFile;
		if (args.length < 4 || args[3] == null || args[3].isEmpty()) {
			// not specified, so load the default from the class path
			URL file = Client.class.getClassLoader().getResource(
					"edu/colorado/eyore/client/resource/client.properties");
			propFile = new File(file.getFile());
		} else {
			propFile = new File(args[3]);
		}
		if (!propFile.exists()) {
			throw new IOException("Properties file not found");
		}

		// load the server properties from the properties file
		Properties props = new Properties();
		props.load(new FileInputStream(propFile));

		// setup logging
		File logConfigFile;
		if (args.length < 5 || args[4] == null || args[4].isEmpty()) {
			URL file = Client.class
					.getClassLoader()
					.getResource(
							"edu/colorado/eyore/vserver/resource/client.log.properties");
			logConfigFile = new File(file.getFile());
		} else {
			logConfigFile = new File(args[4]);
		}
		LogManager.getLogManager().readConfiguration(
				new FileInputStream(logConfigFile));

		// run the client
		Client c = new Client(jarFile, hdfsInputDirPath, hdfsOutputDirPath, props);

		// start client and exit with the status code when finished
		System.exit(c.start());
	}
}
