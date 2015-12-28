package edu.colorado.eyore.client;

import java.beans.XMLDecoder;
import java.io.ByteArrayInputStream;
import java.util.logging.Logger;

import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.job.JobStatus;
import edu.colorado.eyore.common.net.MessageObjectUtil;
import edu.colorado.eyore.common.net.Protocol;

public class JobStatusRequestProtocol implements Protocol {

	Logger logger = Logger.getLogger(JobStatusRequestProtocol.class.getName());
	
	protected boolean isFinished;
	protected JobDescriptor jobDescriptor;
	protected JobStatus jobStatus;
	
	public JobStatusRequestProtocol(JobDescriptor jobDesc){
		isFinished = false;
		this.jobDescriptor = jobDesc;
	}
	
	@Override
	public Boolean isFinished() {
		return isFinished;
	}

	
	@Override
	public String respondTo(String rcvFromFarEnd) {
		// start by sending status request
		if(rcvFromFarEnd == null){
			return "CLIENT REQUEST STATUS:" + MessageObjectUtil.objectToString(jobDescriptor); 
		}
		
		// server responds with OK to request job
		// and info for client to write stuff to HDFS
		// followed by client telling server OK to start
		// the job
		if(rcvFromFarEnd.startsWith("STATUS:")){
			rcvFromFarEnd = rcvFromFarEnd.replace("STATUS:", "");
			
			// Get the JobStatus by converting from XML
			JobStatus jobStatus = MessageObjectUtil.stringToObject(rcvFromFarEnd);
						
			isFinished = true;
			this.jobStatus = jobStatus;
			return null;
		}
		
		throw new IllegalStateException("unexpected response from server " + rcvFromFarEnd);
	}

	@Override
	public Object getData() {
		return jobStatus;
	}

}
