package com.ak.excel_utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ExcelUtils {

	String excelPath;

	public ExcelUtils(String excelPath) {
		this.excelPath = excelPath;
	}

	public static void main(String[] args) {
		ExcelUtils exl = new ExcelUtils("Data.xls");
		List<List<String>> data = Arrays.asList(Arrays.asList("abcom", "a@b.com", "Admin", "123456"),
				Arrays.asList("bccom", "b@c.com", "Admin", "123456"),
				Arrays.asList("cdcom", "c@d.com", "Admin", "123456"),
				Arrays.asList("decom", "d@e.com", "Admin", "123456"));

		System.out.println("Initial Records");
		System.out.println(exl.getAllRecords("Users"));

		exl.deleteAllRecords("Users");
		System.out.println("After deleting all records");
		System.out.println(exl.getAllRecords("Users"));

		exl.insertAllRecords("Users", data);
		System.out.println("After inserting all records");
		System.out.println(exl.getAllRecords("Users"));

		exl.insertRecord("Users", Arrays.asList("decom", "d@e.com", "Admin", "123456"));
		System.out.println("After inserting one records");
		System.out.println(exl.getAllRecords("Users"));

		HashMap<String, String> criteria = new HashMap<String, String>();
		criteria.put("id", "decom");
		exl.updateRecord("Users", criteria, Arrays.asList("decom", "d@e.com", "Panelist", "123456"));
		System.out.println("After updateRecord");
		System.out.println(exl.getAllRecords("Users"));

		exl.deleteRecord("Users", criteria);
		System.out.println("After deleteRecord");
		System.out.println(exl.getAllRecords("Users"));

	}

	public void insertRecord(String tableName, List<String> data) {
		insertAllRecords(tableName, Arrays.asList(data));
	}

	public void insertAllRecords(String tableName, List<List<String>> datas) {
		try {
			FileInputStream file = new FileInputStream(new File(excelPath));

			HSSFWorkbook workbook = new HSSFWorkbook(file);
			HSSFSheet sheet = workbook.getSheet(tableName);

			for (List<String> data : datas) {
				int i = 1;
				while (sheet.getRow(i) != null && sheet.getRow(i).getCell(0) != null) {
					i++;
				}
				System.out.println("Inserting row no : " + i);
				HSSFRow row = sheet.getRow(i);

				if (row == null)
					row = sheet.createRow(i);

				for (int j = 0; j < data.size(); j++) {
					Cell cell = row.getCell(j);
					if (cell == null)
						cell = row.createCell(j);
					cell.setCellValue(data.get(j));
				}
			}
			file.close();

			FileOutputStream outFile = new FileOutputStream(new File(excelPath));
			workbook.write(outFile);
			outFile.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void overwriteAllRecords(String tableName, List<List<String>> datas) {
		deleteAllRecords(tableName);
		insertAllRecords(tableName, datas);
	}

	public void updateRecord(String tableName, Map<String, String> matchingCriteria, List<String> data) {
		try {
			FileInputStream file = new FileInputStream(new File(excelPath));

			HSSFWorkbook workbook = new HSSFWorkbook(file);
			HSSFSheet sheet = workbook.getSheet(tableName);

			List<Integer> matchedRows = getMatchingRows(sheet, matchingCriteria);

			for (Integer rowNo : matchedRows) {
				System.out.println("Updating row : " + rowNo);
				HSSFRow row = sheet.getRow(rowNo);

				if (row == null)
					row = sheet.createRow(rowNo);

				for (int j = 0; j < data.size(); j++) {
					Cell cell = row.getCell(j);
					if (cell == null)
						cell = row.createCell(j);
					cell.setCellValue(data.get(j));
				}
			}

			file.close();

			FileOutputStream outFile = new FileOutputStream(new File(excelPath));
			workbook.write(outFile);
			outFile.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteRecord(String tableName, Map<String, String> matchingCriteria) {
		try {
			FileInputStream file = new FileInputStream(new File(excelPath));

			HSSFWorkbook workbook = new HSSFWorkbook(file);
			HSSFSheet sheet = workbook.getSheet(tableName);

			List<Integer> matchedRows = getMatchingRows(sheet, matchingCriteria);

			for (Integer rowNo : matchedRows) {

				System.out.println("Deleting row : " + rowNo);
				HSSFRow row = sheet.getRow(rowNo);
				sheet.removeRow(row);
			}

			file.close();

			FileOutputStream outFile = new FileOutputStream(new File(excelPath));
			workbook.write(outFile);
			outFile.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void deleteAllRecords(String tableName) {
		try {
			FileInputStream file = new FileInputStream(new File(excelPath));

			HSSFWorkbook workbook = new HSSFWorkbook(file);
			HSSFSheet sheet = workbook.getSheet(tableName);

			int rowNo = 1;
			while (sheet.getRow(rowNo) != null) {

				System.out.println("Deleting row : " + rowNo);
				HSSFRow row = sheet.getRow(rowNo);
				sheet.removeRow(row);
				rowNo++;
			}

			file.close();

			FileOutputStream outFile = new FileOutputStream(new File(excelPath));
			workbook.write(outFile);
			outFile.close();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public List<Map<String, String>> getAllRecords(String tableName) {

		List<Map<String, String>> result = new ArrayList<Map<String, String>>();

		try {
			FileInputStream file = new FileInputStream(new File(excelPath));

			HSSFWorkbook workbook = new HSSFWorkbook(file);
			HSSFSheet sheet = workbook.getSheet(tableName);

			int rowNo = 1;
			while (sheet.getRow(rowNo) != null) {
				Iterator<Cell> header = sheet.getRow(0).cellIterator();

//				System.out.println("Selecting row : " + rowNo);
				HSSFRow row = sheet.getRow(rowNo);
				Iterator<Cell> cellIter = row.cellIterator();
				Map<String, String> data = new HashMap<String, String>();
				while (cellIter.hasNext()) {
					Cell cell = cellIter.next();
					data.put(header.next().getStringCellValue(), getCellValue(cell));
				}
				result.add(data);
				rowNo++;
			}

			file.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return result;
	}

	private String getCellValue(Cell cell) {

		String value = "";

		switch (cell.getCellType()) {
		case BOOLEAN:
			if (cell.getBooleanCellValue())
				value = "True";
			else
				value = "False";
			break;
		case STRING:
			value = cell.getStringCellValue();
			break;
		case NUMERIC:
			value = "" + cell.getNumericCellValue();
			break;
		default:
			value = "";

		}

		return value;

	}

	public List<Map<String, String>> getRecords(String tableName, Map<String, String> matchingCriteria) {

		List<Map<String, String>> result = new ArrayList<Map<String, String>>();

		try {
			FileInputStream file = new FileInputStream(new File(excelPath));

			HSSFWorkbook workbook = new HSSFWorkbook(file);
			HSSFSheet sheet = workbook.getSheet(tableName);

			List<Integer> matchedRows = getMatchingRows(sheet, matchingCriteria);

			for (Integer rowNo : matchedRows) {
				Iterator<Cell> header = sheet.getRow(0).cellIterator();

				System.out.println("Selecting row : " + rowNo);
				HSSFRow row = sheet.getRow(rowNo);
				Iterator<Cell> cellIter = row.cellIterator();
				Map<String, String> data = new HashMap<String, String>();
				while (cellIter.hasNext()) {
					Cell cell = cellIter.next();
					data.put(header.next().getStringCellValue(), cell.getStringCellValue());
				}
				result.add(data);
			}

			file.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return result;
	}

	List<Integer> getMatchingRows(HSSFSheet sheet, Map<String, String> matchingCriteria) {
		List<Integer> matchingRows = new ArrayList<Integer>();
		Map<Integer, String> _matchingCriteria = new HashMap<Integer, String>();

		if (sheet != null && sheet.getRow(0) != null) {
			int i = 0;
			while (sheet.getRow(0).getCell(i) != null) {
				if (matchingCriteria.containsKey(sheet.getRow(0).getCell(i).getStringCellValue())) {
					_matchingCriteria.put(i, matchingCriteria.get(sheet.getRow(0).getCell(i).getStringCellValue()));
				}
				i++;
			}

			int rowNo = 1;
			// Iterate over rows
			while (sheet.getRow(rowNo) != null) {

				// Iterate over criterias
				boolean matched = true;
				for (Integer key : _matchingCriteria.keySet()) {
					if (sheet.getRow(rowNo).getCell(key) != null && sheet.getRow(rowNo).getCell(key)
							.getStringCellValue().equals(_matchingCriteria.get(key))) {
					} else {
						matched = false;
						break;
					}
				}
				if (matched) {
					matchingRows.add(rowNo);
				}

				rowNo++;
			}

		}
//		System.out.println("Column Criterias : ");
//		System.out.println(_matchingCriteria);
//		System.out.println("Matching Rows : ");
//		System.out.println(matchingRows);
		return matchingRows;
	}

}
