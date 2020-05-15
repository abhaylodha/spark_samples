package my.spark.streaming;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Run as : java -classpath
 * F:\Workspaces\Scala\Sample_Spark\build\libs\Sample_Spark.jar
 * my.spark.streaming.Transmitter
 * 
 * @author abhay
 *
 */
public class Transmitter {

	public void transmit() throws IOException, InterruptedException {
		ObjectOutputStream oos = null;
		Socket socket = null;

		try (ServerSocket server = new ServerSocket(5000);) {
			while (true) {
				System.out.println("Waiting for the client request");

				socket = server.accept();

				oos = new ObjectOutputStream(socket.getOutputStream());
				while (true) {

					long millis = System.currentTimeMillis();
					java.util.Date date = new java.util.Date(millis);

					String msg = date.toString() + ", 55";
					oos.writeObject(msg);
					System.out.println("Sent Data : " + msg);

					Thread.sleep(1000);

				}

			}
		} catch (IOException e1) {
			e1.printStackTrace();
		} finally {
			oos.close();
			socket.close();
		}

	}

	public static void main(String[] args) throws IOException, InterruptedException {
		new Transmitter().transmit();
	}

}
