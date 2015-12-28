package edu.colorado.eyore.client;

import java.beans.XMLDecoder;
import java.io.ByteArrayInputStream;
import java.util.logging.Logger;

import edu.colorado.eyore.common.job.JobDescriptor;
import edu.colorado.eyore.common.net.MessageObjectUtil;
import edu.colorado.eyore.common.net.Protocol;

public class RequestJobProtocol implements Protocol {

	Logger logger = Logger.getLogger(RequestJobProtocol.class.getName());
	
	protected boolean isFinished;
	protected JobDescriptor jobDescriptor;
	
	public RequestJobProtocol(){
		isFinished = false;
	}
	
	@Override
	public Boolean isFinished() {
		return isFinished;
	}

	
	@Override
	public String respondTo(String rcvFromFarEnd) {
		// start by requesting a job from the server
		if(rcvFromFarEnd == null){
			return "CLIENT REQUEST JOB:";
		}
		
		// server responds with OK to request job
		// and info for client to write stuff to HDFS
		// followed by client telling server OK to start
		// the job
		if(rcvFromFarEnd.startsWith("OK SUBMIT JOB:")){
			rcvFromFarEnd = rcvFromFarEnd.replace("OK SUBMIT JOB:", "");
			
			// Get the job descriptor by converting from XML to the JobDescriptor object
			JobDescriptor jDesc = MessageObjectUtil.stringToObject(rcvFromFarEnd);
						
			isFinished = true;
			jobDescriptor = jDesc;
			return null;
		}
		
		throw new IllegalStateException("unexpected response from server " + rcvFromFarEnd);
	}

	@Override
	public Object getData() {
		return jobDescriptor;
	}

}
