package com.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * 
 * @author vicky
 *
 */
public class DuplicateDatabase {

	private String url;
	private String user;
	private String password;

	/**
	 * 
	 * @param url
	 * @param user
	 * @param password
	 */
	public DuplicateDatabase(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	/**
	 * This
	 * 
	 * @param existName
	 * @param newName
	 * @return
	 * @throws SQLException
	 * @throws InvalideName
	 */
	public boolean makeDuplicate(String existName, String newName) throws SQLException, InvalideName {

		if (existName == null || existName == "" || newName == null || newName == "") {
			throw new InvalideName("Invalide name");
		}
		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			Statement statement = connection.createStatement();
			String sql = "CREATE DATABASE " + newName + " WITH TEMPLATE " + existName + ";";
			statement.execute(sql);

			return true;
		}

	}

}
