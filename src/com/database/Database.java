package com.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Database {
	private String url;
	private String user;
	private String password;

	/**
	 * 
	 * @param url
	 * @param user
	 * @param password
	 */
	public Database(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;
	}

	/**
	 * 
	 * @return
	 * @throws SQLException
	 */
	public List<String> getSchemas() throws SQLException {
		List<String> schemasName = new ArrayList<String>();
		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			DatabaseMetaData databaseMetaData = connection.getMetaData();

			ResultSet schemas = databaseMetaData.getSchemas();

			while (schemas.next()) {
				if (!schemas.getString(1).equals("information_schema") && !schemas.getString(1).equals("pg_catalog")) {
					String name = schemas.getString(1);
					schemasName.add(name);
				}
			}

			return schemasName;

		}

//		public Connection getConnection() {
//			Connection connection=DriverManager.getConnection(url, user, password);
//			
//			return connection;
//		}

	}

}
