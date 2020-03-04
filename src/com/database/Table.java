package com.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class Table {
	private String url;
	private String user;
	private String password;
	private String tableName;

	/**
	 * 
	 * @param url
	 * @param user
	 * @param password
	 */
	public Table(String url, String user, String password, String tableName) {
		this.url = url;
		this.user = user;
		this.password = password;
		this.tableName = tableName;
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public List<String> getColumnsName() throws SQLException {
		List<String> columnsName = new ArrayList<String>();
		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			Statement statement = connection.createStatement();

			String sql = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME='" + tableName + "'";
			ResultSet columns_name = statement.executeQuery(sql);
			while (columns_name.next())
				columnsName.add(columns_name.getString(1));
			return columnsName;
		}
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public List<String> getColumnsType() throws SQLException {
		List<String> columnsType = new ArrayList<String>();
		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			Statement statement = connection.createStatement();

			String sql = "SELECT  DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME='" + tableName + "'";
			ResultSet data_types = statement.executeQuery(sql);

			while (data_types.next())
				columnsType.add(data_types.getString(1));

			return columnsType;
		}
	}

	/**
	 * 
	 * @param tableName
	 * @return
	 * @throws SQLException
	 */
	public List<String> getRows() throws SQLException {
		List<String> rows = new ArrayList<String>();
		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			Statement statement = connection.createStatement();

			String sql = "SELECT * FROM " + this.tableName;
			ResultSet rs = statement.executeQuery(sql);

			ResultSetMetaData rsMetaData = rs.getMetaData();

			int numOfColumns = rsMetaData.getColumnCount();
			while (rs.next()) {
				String row = "";
				for (int i = 1; i <= numOfColumns; i++) {
					row += rs.getString(i);
					if (i == numOfColumns) {
						continue;
					}
					row += ",";

				}

				rows.add(row);

			}
			return rows;
		}
	}

	boolean clear() throws SQLException {

		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			Statement statement = connection.createStatement();

			String sql = "delete from " + this.tableName;

			return statement.execute(sql);
		}

	}

	public void update(List<String> rows) throws SQLException {

		List<String> columnsName = getColumnsName();
		List<String> columnsType = getColumnsType();

		String columnName = mergeColumns(columnsName);
		String columnValue = mergeTypes(columnsType);

		clear();
		String sql = "insert into " + tableName + "(" + columnName + ")" + " values(" + columnValue + ")";
		// System.out.println(sql);
		try (Connection connection = DriverManager.getConnection(url, user, password)) {

			PreparedStatement ps = connection.prepareStatement(sql);
			int count = 0;
			for (String row : rows) {

				String data[] = row.split(",");
				for (int i = 1; i <= columnsType.size(); i++) {

					if (columnsType.get(i - 1).equals("integer")) {
						ps.setInt(i, Integer.parseInt(data[i - 1]));
					}
					if (columnsType.get(i - 1).equals("text")) {
						ps.setString(i, data[i - 1]);
					}
					if (columnsType.get(i - 1).equals("double precision")) {
						ps.setDouble(i, Double.parseDouble(data[i - 1]));
					}
				}

				ps.addBatch();
				count++;

				// Execute every 100 rows or less
				if (count % 100 == 0 || count == rows.size()) {
					ps.executeBatch();
				}

			}

		}

	}

	String getSQL() throws SQLException {
		List<String> columnsName = getColumnsName();
		List<String> columnsType = getColumnsType();

		String columnName = mergeColumns(columnsName);
		String columnValue = mergeTypes(columnsType);

		String sql = "insert into " + tableName + "(" + columnName + ")" + "values(" + columnValue + ")";
		return sql;
	}

	String mergeColumns(List<String> list) {
		String str = "";

		for (int i = 0; i < list.size(); i++) {
			str += list.get(i);
			if (i == list.size() - 1) {
				continue;
			}

			str += ",";
		}

		return str;
	}

	String mergeTypes(List<String> list) {
		String str = "";

		for (int i = 0; i < list.size(); i++) {
			str += "?";
			if (i == list.size() - 1) {
				continue;
			}

			str += ",";
		}

		return str;
	}
}
