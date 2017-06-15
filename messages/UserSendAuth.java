/**
 * Created by mercenery on 15.06.2017.
 */
public class UserSendAuth{
	int    toDoCode = 0;
	String login    = null;
	String pass     = null;
	String name     = null;
	
	public UserSendAuth(int toDoCode, String login, String pass, String name){
		this.toDoCode = toDoCode;
		this.login = login;
		this.pass = pass;
		this.name = name;
	}
}
