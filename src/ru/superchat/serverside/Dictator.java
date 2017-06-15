package ru.superchat.serverside;

import entries.EntryDic;
import massage.UserSendAuth;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.sql.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by mercenery on 02.06.2017.
 */

public class Dictator{
	private static UserSendAuth    authMes;
	private static ExecutorService authorization;
	private static ExecutorService connectToChat;
	private static ExecutorService startNewAuth;
	
	static Socket          socket;
	
	/**
	 *
	 * @param args
	 */
	public static void main(String[] args){
// открываем порт для подключения
		try(ServerSocket serverSocket = new ServerSocket(23345);
		    ObjectOutputStream objectOutputStream = new ObjectOutputStream(socket.getOutputStream());
		    ObjectInputStream objectInputStream = new ObjectInputStream(socket.getInputStream())){
			
			while(!serverSocket.isClosed()){
			
// читаем обращение из канала
				authMes = (UserSendAuth)objectInputStream.readObject();

// с помощью делегата для общения с базой - Callable объекта - DBServerHandler запрашиваем базу, передавая
// объект с авторизацией  в конструкторе DBServerHandler и получаем ответ от DBServerHandler
				Future<Integer> authoRequest = authorization.submit(new DBServerHandler(authMes));

				int authReply = authoRequest.get();
				
				if(authReply == 1){

// если учётная запись есть  = 1 - стартует новая нить - отправляем сокет в чат
					connectToChat.execute(ToChat(socket));
				}
				else {

// если юзеру нужно пройти авторизацию то передаём его сокет в нить авторизации и в чат он попадает из ннеё
					startNewAuth.execute(ToAuth(socket));
				}
			}
			authorization.shutdown();
			connectToChat.shutdown();
			startNewAuth.shutdown();
			socket.close();


// закрываем пулs нитей
		} catch(IOException e) {
			e.printStackTrace();
		} catch(InterruptedException e) {
			e.printStackTrace();
		} catch(ClassNotFoundException e) {
			e.printStackTrace();
		} catch(ExecutionException e) {
			e.printStackTrace();
		}
	}
}
