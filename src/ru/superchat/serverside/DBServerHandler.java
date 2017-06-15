package ru.superchat.serverside;

import entries.EntryDic;
import massage.UserSendAuth;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.concurrent.Callable;

import static entries.EntryDic.*;

/**
 * Created by mercenery on 02.06.2017.
 */


public class DBServerHandler implements Callable<UserSendAuth>{
	
	private static Connection connection;
	
	private static PreparedStatement pSDelete;
	private static PreparedStatement pSSearch;
	private static PreparedStatement pSAdd;
	private static PreparedStatement pSReduct;
	
	static FileWriter fileErrorLogWriter;
	
	
	/**
	 * Конструктор. Присваивает переданный в параметрах<code>entryDic</code> статическому полю с таким же названием.
	 * Создаёт лог файл, переменную отвечающую за логирование, загружает драйвер
	 * базы с которой будет работать объект, соединение и создаёт предскомпилированные запросы для отправки команд базе.
	 *
	 * @param entryDic
	 */
	
	public DBServerHandler(EntryDic entryDic){
		this.entryDic = entryDic;
		System.out.println("DBServerHandler : DBServerHandler constructor starts");
		
		try{
			File file = new File("DBServerHandler_error_log.txt");
			fileErrorLogWriter = new FileWriter(file);
			System.out.println("DBServerHandler : logfile in DBServerHandler created , \nDB driver loading starts");
			
			Class.forName("org.postgresql.Driver");
			System.out.println("DBServerHandler : DB driver in DBServerHandler loaded");
			
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/dictionary", "postgres",
			                                         "postgres");
			
			pSDelete = connection.prepareStatement("delete from word_define where " + "UPPER(word) = UPPER(?);");
			pSSearch = connection.prepareStatement(
				"select definition from word_define where UPPER(word) like UPPER(?);");
			pSAdd = connection.prepareStatement("insert into word_define(word, definition) values(?,?);");
			pSReduct = connection.prepareStatement(
				"update word_define set definition = ? where UPPER(word) = UPPER(?);");
			
			System.out.println("DBServerHandler : preparedstatements & connection created in DBServerHandler");
		} catch(SQLException e) {
			String errlog = "DBServerHandler : SQL statement PREparing or DB connection establish troubles\n" + e
				.getMessage();
			try{
				fileErrorLogWriter.write(errlog);
				System.err.println(errlog);
			} catch(IOException e1) {
				System.err.println(
					"DBServerHandler : DBServerHandler_error_log.txt file creating troubles\n" + e.getMessage());
				System.exit(1);
			}
			System.exit(1);
		} catch(ClassNotFoundException e) {
			String errlog = "DBServerHandler : DB driver loading troubles\n" + e.getMessage();
			try{
				fileErrorLogWriter.write(errlog);
				System.err.println(errlog);
			} catch(IOException e1) {
				System.err.println(
					"DBServerHandler : DBServerHandler_error_log.txt file creating troubles\n" + e1.getMessage());
			}
			System.exit(1);
		} catch(IOException e) {
			System.err.println(
				"DBServerHandler : DBServerHandler_error_log.txt file creating troubles\n" + e.getMessage());
			System.exit(1);
		}
	}
	
	/**
	 * Computes a result, or throws an exception if unable to do so.
	 * <p>
	 * <p>
	 * Возвращающий метод - <code>call</code> вызывает - метод <code>toDoSelector</code>, которому в качестве аргумента
	 * передаёт ссылочную переменную типа - <code>EntryDic</code> полученную в консрукторе <code>DBServerHandler</code>.
	 * <code>DBServerHandler</code> в свою очередь возвращает другой <code>EnryDic</code> для ответной передачи его
	 * запросившему клиенту в объект <code>CSDialog</code>.
	 * <p>
	 * Предусмотрено логирование аварийного завершения выполнения операции в файл <code>"DBServerHandler_error_log.txt"</code>
	 * <code>file</code> переменная.
	 *
	 * @return computed result
	 */
	@Override
	public EntryDic call(){
		System.out.println("DBServerHandler : call method in DBServerHandler starts");
		EntryDic entryFromToDo = null;
		try{
			entryFromToDo = toDoSelector(entryDic);
			System.out.println(
				"DBServerHandler : answer in call method from method toDoSelector in DBS granted " + "EntryDic is: "
				+ entryFromToDo.flag + " " + entryFromToDo.word + " " + entryFromToDo.definition);
		} catch(InterruptedException e) {
			e.printStackTrace();
		}
		System.out.println("DBServerHandler : EntryDic from call method in DBServerHandler returning to CSDialog ");
		return entryFromToDo;
	}
	
	
	/**
	 * Метод - селектор - получает параметром объект типа <code>EntryDic</code>, сохраняет его в своей локальной
	 * переменной и проверяет чему равен переданный с объектом флаг операции(удалить - <code>DELETE</code> существующую
	 * запись, 	найти - <code>SEARCH</code> определение по ключевому! слову,  добавить - <code>ADD</code> новую запись,
	 * корректировать существующую запись - <code>REDUCT</code>), и вызывает соответствующие методы, передавая им в
	 * параметры копию ссылки на полученный объект типа <code>EntryDic</code>.
	 * <p>
	 * Предусмотрено логирование аварийного завершения выполнения операции в файл <code>"DBServerHandler_error_log.txt"</code>
	 * <code>file</code> переменная.
	 *
	 * @param entryDic
	 * @return
	 */
	
	public EntryDic toDoSelector(EntryDic entryDic) throws InterruptedException{
		System.out.println("DBServerHandler : ToDoSelector in DBServerHandler works");
		EntryDic entryDicIn  = entryDic;
		EntryDic entryDicOut = null;
		int      flag        = entryDic.flag;
		System.out.println(
			"DBServerHandler : input entryDic in selector in DBServerHandler - " + entryDicIn.flag + "" + " "
			+ entryDicIn.word + " " + entryDicIn.definition);
		switch(flag){
			case - 1:
				System.out.println(
					"DBServerHandler : found DELETE - flag in ToDoSelector in DBServerHandler, try to delete entry");
				System.out.println("DBServerHandler : deleting entry with keyword - " + entryDicIn.word);
				entryDicOut = deleteEntry(entryDicIn);
				break;
			case 0:
				System.out.println(
					"DBServerHandler : found SEARCH - flag in ToDoSelector in DBServerHandler, try to SEARCH entry");
				System.out.println("DBServerHandler : searching entry with keyword - " + entryDicIn.word);
				
				entryDicOut = searchEntry(entryDicIn);
				System.out.println(
					"DBServerHandler : return into method toDoSelector from method  searchEntry " + "EntryDic is : "
					+ entryDicOut.flag + " " + entryDic.word + " " + entryDic.definition);
				break;
			case 1:
				System.out.println(
					"DBServerHandler : found ADD - flag in ToDoSelector in DBServerHandler, try to ADD entry");
				System.out.println("DBServerHandler : adding entry with keyword - " + entryDicIn.word);
				entryDicOut = addEntry(entryDicIn);
				break;
			case 10:
				System.out.println(
					"DBServerHandler : found REDUCT - flag in method ToDoSelector in DBServerHandler, try to REDUCT "
					+ "entry");
				System.out.println("DBServerHandler : reducting entry with keyword - " + entryDicIn.word);
				entryDicOut = reductEntry(entryDicIn);
				break;
		}
		System.out.println(
			"DBServrHandler : ready to return answer from method toDoSelector in DBServrHandler to call method in"
			+ " DBServrHandler the EntryDic is : " + entryDicOut.flag + " " + entryDic.word + " "
			+ entryDic.definition);
		return entryDicOut;
	}
	
	/**
	 * SQL запрос к таблице <code>"word_define"</code> в базе данных с именем - <code>"dictionary"</code>, в случае
	 * удачного удаления, выполняется код в фигурных скобках в которых происходит <code>return </code>преобразованного
	 * <code>EntryDic</code> в котором флаг операции <code>flag</code> не изменяется, а в полях <code>word </code>и
	 * <code>definition</code> пишется "ок " - подтверждение удачного выполнения удаления, в случае если удаление не
	 * произошло то выполнение из круглых скобок не попадает в фигурные, а падает в (<code>SQLException</code>)
	 * ниже , в котором происходит формирование нового (<code>new EntryDic();</code>)в котором флаг операции
	 * не меняется, а в полях word и definition выводится - error.
	 * <p>
	 * Предусмотрено логирование аварийного завершения выполнения операции в файл <code>"DBServerHandler_error_log.txt"</code>
	 * <code>file</code> переменная.
	 *
	 * @param entryDic
	 * @return
	 */
	
	public EntryDic deleteEntry(EntryDic entryDic){
		System.out.println("DBServerHandler : starts deleteEntry - returnable method in DBServerHandler");
		EntryDic entryDicDelete;
		String   tmpWord = entryDic.word;
		try{
			pSDelete.setString(1, tmpWord);
			pSDelete.executeUpdate();
			
			System.out.println("DBServerHandler : prepared statement send in deleteEntry method in DBS");
			pSDelete.close();
			connection.close();
			
			System.out.println("DBServerHandler : preparedstatements & connection closed in deleteEntry method in DBS");
			entryDicDelete = new EntryDic(- 1, "ok, deleted", "ok, deleted");
			System.out.println(
				"DBServerHandler : EntryDic reply from DBServerHandler after deleteEntry method granted = "
				+ entryDicDelete.flag + " " + entryDicDelete.word + " " + entryDicDelete.definition);
			return entryDicDelete;
		} catch(SQLException e) {
			entryDicDelete = new EntryDic(- 1, "error deleting", "error deleting");
			System.err.println("DBServerHandler : pSDelete.executeUpdate(); troubles\n" + e.getMessage());
			return entryDicDelete;
		} finally{
			try{
				pSDelete.close();
				connection.close();
			} catch(SQLException e) {
				String errLog = "DBServerHandler : i/o channels.close(); in socket or connection.close(); troubles\n"
				                + e.getMessage();
				System.err.println(errLog);
				try{
					fileErrorLogWriter.write(errLog);
				} catch(IOException e1) {
					System.err.println(
						"DBServerHandler : error log file writing operation troubles\n" + e1.getMessage());
				}
			}
		}
		
	}
	
	/**
	 * SQL запрос к таблице <code>"word_define"</code> в базе данных с именем - <code>"dictionary"</code>,
	 * в случае удачного поиска определения <code>definition</code>, выполняется код в фигурных скобках в которых
	 * происходит <code>return</code> преобразованного <code>EntryDic</code> в котором флаг операции <code>flag</code> и
	 * поле <code>word</code> не изменяются, а в и definition пишется "определение" - по введённому слову, в случае
	 * если поиск не успешен то - выполнение из круглых скобок не попадает в фигурные, а падает в
	 * <code>SQLException</code>  ниже , в котором происходит формирование нового
	 * <code>new EntryDic ();</code> в котором флаг операции <code>flag</code> не меняется, а в полях флаг операции
	 * <code>word</code> и <code>definition</code> выводится - "error".
	 * <p>
	 * Предусмотрено логирование аварийного завершения выполнения операции в файл <code>"DBServerHandler_error_log.txt"</code>
	 * <code>file</code> переменная.
	 *
	 * @param entryDic
	 * @return
	 */
	
	private EntryDic searchEntry(EntryDic entryDic){
		System.out.println("DBServerHandler : starts returnable method searchEntry in DBServerHandler");
		String   tmpWord        = entryDic.word;
		EntryDic entryDicSearch = new EntryDic(0, "0", "0");
		String   tmpDefinitionBack;
		
		try{
			pSSearch.setString(1, tmpWord);
			System.out.println("DBServerHandler : prepared statement send in searchEntry method in DBServerHandler ");
			
			ResultSet resultSearch = pSSearch.executeQuery();
//			pSSearchCloseKey.executeUpdate("update keyclose set isclosed='false';");
			resultSearch.next();
			tmpDefinitionBack = resultSearch.getString("definition");
			
			System.out.println(
				"DBServerHandler : resultset granted in searchEntry method in DBServerHandler " + tmpDefinitionBack);
			
			resultSearch.close();
			pSSearch.close();
			
			System.out.println("DBServerHandler : resultset & preparementstatement were closed in searchEntry method "
			                   + "in DBServerHandler ");
			entryDicSearch.flag = 0;
			entryDicSearch.word = tmpWord;
			entryDicSearch.definition = tmpDefinitionBack;
			System.out.println("DBSServerHandler : definition from DB granted - " + tmpDefinitionBack + " returning "
			                   + "from method - searchEntry to method toDoSelector in DBServerHandler");
			return entryDicSearch;
		} catch(SQLException e) {
			System.err.println("DBServerHandler : SQL query or pSSearch.close(); troubles\n" + e.getMessage());
			return new EntryDic(0, "error searching", "error searching");
		} finally{
			try{
				pSSearch.close();
				connection.close();
			} catch(SQLException e) {
				String errLog = "DBServerHandler : DB resources .close(); or connection.close(); troubles\n" + e
					.getMessage();
				System.err.println(errLog);
				try{
					fileErrorLogWriter.write(errLog);
				} catch(IOException e1) {
					System.err.println(
						"DBServerHandler : error log file writing operation troubles\n" + e.getMessage());
				}
			}
		}
	}
	
	
	/**
	 * SQL запрос к таблице <code>"word_define"</code> в базе данных с именем - <code>"dictionary"</code>, в случае
	 * удачного выполнения операции, выполняется код в фигурных скобках в которых происходит <code>return</code>
	 * преобразованного <code>EntryDic</code> , в котором флаг операции <code>flag</code> не изменяются, а в
	 * поле <code>word</code> и <code>definition</code> пишется "ok" - подтверждение добавления новой записи слову,
	 * в случае если добавление не произошло то выполнение из круглых скобок не попадает
	 * в фигурные, а падает в <code>SQLException</code>ниже , в котором происходит формирование
	 * нового <code>new EntryDic();</code>, в котором флаг операции <code>flag</code> не изменяются, а в полях
	 * <code>word</code> и <code>definition</code> выводится - "error".
	 * <p>
	 * Предусмотрено логирование аварийного завершения выполнения операции в файл <code>"DBServerHandler_error_log.txt"</code>
	 * <code>file</code> переменная.
	 *
	 * @param entryDic
	 * @return
	 */
	
	public EntryDic addEntry(EntryDic entryDic){
		System.out.println("DBServerHandler : starts addEntry returnable method in DBServerHandler");
		EntryDic entryDicAdd;
		String   tmpWord       = entryDic.word;
		String   tmpDefinition = entryDic.definition;
		
		try{
			pSAdd.setString(1, tmpWord);
			pSAdd.setString(2, tmpDefinition);
			pSAdd.executeUpdate();
			System.out.println("DBServerHandler : preparedstatements sent in addEntry in DBServerHandler ");
			
			entryDicAdd = new EntryDic(1, "ok, " + tmpWord + " added", "ok, " + tmpDefinition + " added");
			
			
			pSAdd.close();
			connection.close();
			System.out.println("DBServerHandler : resources in addEntry method in DBServerHandler had been closed");
			return entryDicAdd;
		} catch(SQLException e) {
			System.err.println("DBServerHandler : SQL query or SQL closing troubles\n" + e.getMessage());
			System.exit(1);
			entryDicAdd = new EntryDic(1, "error adding", "error adding");
			return entryDicAdd;
		} finally{
			try{
				pSAdd.close();
				connection.close();
			} catch(SQLException e) {
				try{
					String errLog = "DBServerHandler : DB resources .close(); or connection.close(); troubles\n" + e
						.getMessage();
					fileErrorLogWriter.write(errLog);
				} catch(IOException e1) {
					System.err.println(
						"DBServerHandler : error log file writing operation troubles\n" + e.getMessage());
				}
			}
		}
	}
	
	/**
	 * SQL запрос к таблице <code>"word_define"</code> в базе данных с именем - <code>"dictionary"</code>, в случае
	 * удачного выполнения операции, выполняется код в фигурных скобках в которых происходит <code>return</code>
	 * преобразованного <code>EntryDic</code> , в котором флаг операции <code>flag</code> не изменяются, а в
	 * поле <code>word</code> и <code>definition</code> пишется "ok" - подтверждение удачного завершения
	 * редактирования, в случае если редактирование  не произошло - то выполнение из круглых скобок не попадает в
	 * фигурные, а падает в <code>SQLException</code> ниже, в котором происходит формирование нового
	 * <code>new EntryDic();</code>, в котором флаг операции <code>flag</code> не изменяются, а в полях
	 * <code>word</code> и <code>definition</code> выводится - "error".
	 * <p>
	 * Предусмотрено логирование аварийного завершения выполнения операции в файл <code>"DBServerHandler_error_log.txt"</code>
	 * <code>file</code> переменная.
	 *
	 * @param entryDic
	 * @return
	 */
	private EntryDic reductEntry(EntryDic entryDic){
		System.out.println("DBServerHandler : starts reductEntry returnable method in DBServerHandler");
		EntryDic entryDicReduct;
		String   tmpWord       = entryDic.word;
		String   tmpDefinition = entryDic.definition;
		
		try{
			pSReduct.setString(1, tmpDefinition);
			pSReduct.setString(2, tmpWord);
			pSReduct.executeUpdate();
			System.out.println("DBServerHandler : preparedstatements in reductEntry method in DBServerHandler sent");
			
			pSReduct.close();
			connection.close();
			System.out.println("DBServerHandler : resources closed in reductEntry method in DBServerHandler closed");
			
			entryDicReduct = new EntryDic(10, "ok , reducted", "ok , reducted");
			return entryDicReduct;
		} catch(SQLException e) {
			System.err.println("DBServerHandler : pSReduct executing or pSReduct.close() troubles\n" + e.getMessage());
			entryDicReduct = new EntryDic(10, "error , reducted", "error , reducted");
			return entryDicReduct;
		} finally{
			try{
				pSReduct.close();
				connection.close();
			} catch(SQLException e) {
				String errLog = "DBServerHandler : DB resources .close(); or connection.close(); troubles\n" + e
					.getMessage();
				System.err.println(errLog);
				try{
					fileErrorLogWriter.write(errLog);
				} catch(IOException e1) {
					System.err.println(
						"DBServerHandler : error log file writing operation troubles\n" + e.getMessage());
				}
			}
		}
	}
}