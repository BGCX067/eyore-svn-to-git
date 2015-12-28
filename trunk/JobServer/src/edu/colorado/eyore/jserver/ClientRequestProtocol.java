package edu.colorado.eyore.jserver;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.colorado.eyore.common.JarClassLoadUtil;
import edu.colorado.eyore.common.hdfs.HdfsUtils;
import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.job.JobSpecification;
import edu.colorado.eyore.common.job.JobStatus;
import edu.colorado.eyore.common.net.MessageObjectUtil;
import edu.colorado.eyore.common.net.Protocol;

public class ClientRequestProtocol implements Protocol {
	
	Logger logger = Logger.getLogger(ClientRequestProtocol.class.getName());

	private JobServer jobServerRef;
	private JobDescriptor jDesc;
	private boolean isFinished;
	private HdfsUtils hdfs;
	
	public ClientRequestProtocol(JobServer jobServerRef, HdfsUtils hdfs) {
		isFinished = false;
		this.jobServerRef = jobServerRef;
		this.hdfs = hdfs;
	}
	
	@Override
	public Object getData() {
		return jDesc;
	}

	@Override
	public Boolean isFinished() {
		return isFinished;
	}

	@Override
	public String respondTo(String rcvFromFarEnd) {
		logger.fine("RCV: " + rcvFromFarEnd);
		
		// handle client is requesting to run a job (for
		// the first time) - need to give client a unique
		// id for the job
		if(rcvFromFarEnd.startsWith("CLIENT REQUEST JOB:")){
			JobDescriptor jDesc = new JobDescriptor();
			
			jDesc.setJobId(jobServerRef.getUniqueJobId() + "");

			String jobDescriptorAsString = MessageObjectUtil.objectToString(jDesc);
			
			this.isFinished = true;

			// Message includes decodable job descriptor object
			String resp =  "OK SUBMIT JOB:" + jobDescriptorAsString;
			
			logger.fine("SEND:" + resp);
			return resp;
		}else if(rcvFromFarEnd.startsWith("CLIENT REQUEST JOB START:")){
			jDesc = MessageObjectUtil.stringToObject(
					rcvFromFarEnd.replaceFirst("CLIENT REQUEST JOB START:", ""));
			
			JobSpecification jSpec = null;
			try{
				jSpec = loadJobSpec(jDesc);
				
				// set the input & output hfds dir to the client command line specified HDFS directory
				jSpec.setInputPath(jDesc.getHdfsJobInputDir());
				jSpec.setOutputPath(jDesc.getHdfsJobOutputDir());
			}catch(Exception e){

				logger.log(Level.SEVERE, "Failed to load JobSpecification from jar", e);
				jDesc = null;
				isFinished = true;
				return "JOB FAILED TO START:" + e.getMessage().replace("\n", " ");
			}

			isFinished = true;
			jDesc.setJobSpecification(jSpec);
			return "JOB STARTED:";
		}else if(rcvFromFarEnd.startsWith("CLIENT REQUEST STATUS:")){
			jDesc = MessageObjectUtil.stringToObject(rcvFromFarEnd.replaceFirst("CLIENT REQUEST STATUS:", ""));
			JobStatus jobStatus = jobServerRef.jobManager.jobStatusQuery(jDesc.getJobId());
			if(jobStatus == null){
				return "NOT FOUND";
			}
			String jobStatusAsString = MessageObjectUtil.objectToString(jobStatus);
			return "STATUS:" + jobStatusAsString;
			
		}else{
			logger.severe("Received unexpected client msg:" + rcvFromFarEnd);
		}

		return null;
	}
	
	protected JobSpecification loadJobSpec(JobDescriptor jobDescriptor)throws IOException{
		logger.fine("start");
		File localJarCopy = null;

		logger.fine("create temp file");
		localJarCopy = File.createTempFile("jobServer-tempLocalJar-", "");
		
		logger.fine("copy jar from HDFS to local file system");
		hdfs.writeHdfsFileToLocal(jobDescriptor.getHdfsJarPath(), localJarCopy);
		
		if(! localJarCopy.exists()){
			throw new IOException("failed making local copy of jar file ");
		}
		
		List<Class> classes = JarClassLoadUtil.loadClassFromJar(JobSpecification.class, localJarCopy);
		if(! (classes.size() == 1)){
			throw new IllegalStateException("Expected exaclty one JobSpecification instance - found "
					+ classes.size() + " instances instead. Local jar: " + localJarCopy.getAbsolutePath());
		}
		logger.fine("Successfully loaded JobSpecification from local jar");
		Class<JobSpecification> jspec = classes.get(0);
		
		return JarClassLoadUtil.getInstanceFromClass(jspec);
	}

}
