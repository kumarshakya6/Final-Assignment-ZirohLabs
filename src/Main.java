import java.sql.SQLException;
import java.util.List;

import com.database.Database;
import com.database.DuplicateDatabase;
import com.database.InvalideName;
import com.database.Modifier;
import com.database.Schema;
import com.database.Table;

public class Main {

	public static void main(String[] args) throws SQLException, InvalideName {

		String user = "postgres";
		String url = "jdbc:postgresql://127.0.0.1:5555/";
		String password = "Shakya@2020";

		String existName = "test1";
		String newName = "newtest1";

		String catalog = null;
		String schemaPattern = null;
		String tableNamePattern = null;
		String types[] = null;

//		Schema schema = new Schema(url, user, password);
//		List<String> tablesName = schema.getTablesName(catalog, schemaPattern, tableNamePattern, types);
//		for (String tableName : tablesName) {
//			System.out.println(tableName);
//		}

		DuplicateDatabase duplicateDatabase = new DuplicateDatabase(url, user, password);

		duplicateDatabase.makeDuplicate(existName, newName);

		System.out.println("Schemas name....................");

		// Add database name into the URL
		url += newName;

		Database database = new Database(url, user, password);

		List<String> schemasName = database.getSchemas();

		System.out.println("All Schemas ..........");
		for (String sn : schemasName) {
			System.out.println(sn);
		}

		// Accessing all the schemas one by one
		for (String schemaName : schemasName) {

			schemaPattern = schemaName;

//			System.out.println("Schema " + schemaPattern);

			Schema schema = new Schema(url, user, password);
			List<String> tablesName = schema.getTablesName(catalog, schemaPattern, tableNamePattern, types);

			System.out.println("All Tables ..........");

			for (String tn : tablesName) {
				System.out.println(tn);
			}

			// Accessing all the tables one by one
			for (String tableName : tablesName) {

//				System.out.println("Table " + tableName);

				Table table = new Table(url, user, password, tableName);

				List<String> columnsType = table.getColumnsType();
				List<String> columnNames = table.getColumnsName();
				List<String> rows = table.getRows();
				for (int i = 0; i < columnNames.size(); i++) {
					System.out.println(columnNames.get(i) + " " + columnsType.get(i));
				}

				Modifier modifier = new Modifier("ZirohLabs", 100);

				List<String> updatedRows = modifier.modifyRows(columnsType, rows);

				for (String row : updatedRows) {
					System.out.println(row);
				}

				table.update(updatedRows);

			}
		}

	}

}
