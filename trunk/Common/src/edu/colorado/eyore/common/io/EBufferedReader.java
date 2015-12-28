package edu.colorado.eyore.common.io;

import java.io.BufferedReader;
import java.io.Reader;

/**
 * A common BufferedReader for Eyore that uses a changeable
 * buffer size 
 */
public class EBufferedReader extends BufferedReader {

	//private static final int BUFF_BYTES = 4*104857;
	
	public EBufferedReader(Reader in) {
		super(in);//, BUFF_BYTES);
	}

}
