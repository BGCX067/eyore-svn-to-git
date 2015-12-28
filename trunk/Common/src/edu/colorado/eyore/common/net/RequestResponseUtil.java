package edu.colorado.eyore.common.net;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;


public class RequestResponseUtil {

	/**
	 * A client (either the "cient" or the vertex server) makes a request to a
	 * server ("job server") using a socket and receives a response
	 * 
	 * @param <T>
	 *            getData() value of protocol after response from server
	 *            received
	 * @param serverAddr
	 * @param serverPort
	 * @return
	 * @throws IOException
	 */
	public static <T> T clientRequest(Protocol p, String serverAddr,
			int serverPort) throws IOException {
		Socket toServer;
		try {
			toServer = new Socket(serverAddr, serverPort);
		} catch (IOException e) {
			throw new RuntimeException("Failed to connect to server on "
					+ serverAddr + ":" + serverPort, e);
		}

		try {
			PrintWriter out = new PrintWriter(toServer.getOutputStream());
			BufferedReader in = new BufferedReader(new InputStreamReader(
					toServer.getInputStream()));

			// client initiates request
			out.println(p.respondTo(null));
			out.flush();

			// client gets response
			p.respondTo(in.readLine());

			// communication is over now

		} finally {
			toServer.close();
		}
		return (T) p.getData();
	}

	public static <T> T serverResponse(Protocol p, Socket s, String request) throws IOException {
		try {
			PrintWriter out = new PrintWriter(s.getOutputStream());
			
			out.println(p.respondTo(request));
			out.flush();

			s.close();

			return (T) p.getData();
		} finally {
			s.close();
		}
	}
}
