package edu.colorado.eyore.common.io;

import java.io.BufferedWriter;
import java.io.Writer;

/**
 * A BufferedWriter that allows us to control buffer size 
 */
public class EBufferedWriter extends BufferedWriter {

	//private static final int BUFF_BYTES = 4*104857;
	
	public EBufferedWriter(Writer out) {
		super(out);//, BUFF_BYTES);
	}

}
