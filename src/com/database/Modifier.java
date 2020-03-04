package com.database;

import java.util.ArrayList;
import java.util.List;

public class Modifier {
	String mstr;
	int mint;

	/**
	 * 
	 * @param mstr
	 * @param mint
	 */
	public Modifier(String mstr, int mint) {
		this.mstr = mstr;
		this.mint = mint;
	}

	/**
	 * This method modify the given rows only Integer and String Columns after that
	 * returns the rows
	 * 
	 * @param columnsType
	 * @param rows
	 * @return
	 */
	public List<String> modifyRows(List<String> columnsType, List<String> rows) {

		List<String> updatedRows = new ArrayList<String>();
		for (String row : rows) {

			String data[] = row.split(",");

			for (int i = 0; i < data.length && i < columnsType.size(); i++) {

				if (columnsType.get(i).equals("integer")) {
					int value = Integer.parseInt(data[i]) + mint;
					data[i] = Integer.toString(value);

				}
				if (columnsType.get(i).equals("text")) {
					String newdata = data[i] + mstr;
					data[i] = newdata;
				}

			}
			String out = "";
			for (int i = 0; i < data.length; i++) {
				out += data[i];
				if (i == data.length - 1) {
					continue;
				}
				out += ",";
			}

			// System.out.println(out);
			updatedRows.add(out);
		}

		return updatedRows;
	}

}
