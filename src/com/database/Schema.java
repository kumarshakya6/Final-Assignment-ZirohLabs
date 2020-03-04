package com.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class Schema {
	private String url;
	private String user;
	private String password;

	/**
	 * 
	 * @param url
	 * @param user
	 * @param password
	 */
	public Schema(String url, String user, String password) {
		this.url = url;
		this.user = user;
		this.password = password;

	}

	/**
	 * 
	 * @param catalog
	 * @param schemaPattern
	 * @param tableNamePattern
	 * @param types
	 * @return List
	 * @throws SQLException
	 */
	public List<String> getTablesName(String catalog, String schemaPattern, String tableNamePattern, String types[])
			throws SQLException {
		List<String> tablesName = new ArrayList<String>();
		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			DatabaseMetaData databaseMetaData = connection.getMetaData();
			ResultSet tables = databaseMetaData.getTables(catalog, schemaPattern, tableNamePattern, types);

			while (tables.next()) {
				String name = tables.getString(3);

				tablesName.add(name);

			}

			return tablesName;

		}

	}

}
