package ru.superchat.serverside;

import massage.UserSendAuth;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * Created by mercenery on 03.06.2017.
 */
public class CSDialog implements Runnable{
	
	private static Socket socket;
	
	/**
	 * @param socketFromDictator
	 */
	
	public CSDialog(Socket socketFromDictator) throws InterruptedException{
		this.socket = socketFromDictator;
	}
	
	/**
	 * @see Thread#run()
	 */
	@Override
	public void run(){
		System.out.println("CSDialog : CSDialog run() method starts");
		try(ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
		    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())){
			while(! socket.isClosed()){
				
				/* TODO написать логику общения*/
				
			}
			socket.close();
			System.out.println("CSDialog : socket in CSDialog closed");
			
		} catch(IOException e1)
		
		{
			System.err.println("CSDialog : IO troubles\n" + e1.getMessage());
			System.exit(1);
		}
	}
}
