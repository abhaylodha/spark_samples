package chart_creator;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xddf.usermodel.chart.AxisCrosses;
import org.apache.poi.xddf.usermodel.chart.AxisPosition;
import org.apache.poi.xddf.usermodel.chart.BarDirection;
import org.apache.poi.xddf.usermodel.chart.ChartTypes;
import org.apache.poi.xddf.usermodel.chart.LegendPosition;
import org.apache.poi.xddf.usermodel.chart.XDDFBarChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFCategoryAxis;
import org.apache.poi.xddf.usermodel.chart.XDDFChartData;
import org.apache.poi.xddf.usermodel.chart.XDDFChartLegend;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFDataSourcesFactory;
import org.apache.poi.xddf.usermodel.chart.XDDFNumericalDataSource;
import org.apache.poi.xddf.usermodel.chart.XDDFValueAxis;
import org.apache.poi.xssf.usermodel.XSSFChart;
import org.apache.poi.xssf.usermodel.XSSFClientAnchor;
import org.apache.poi.xssf.usermodel.XSSFDrawing;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class ApachePoi {

	public static void main(String[] args) throws IOException {

		String tableName = "Table_1";
		String title = "Area-wise Top Seven Countries";
		String bottomAxis = "Country";
		String leftAxis = "Area";
		String seriesTitle = "Country";

		List<List<Object>> data = Arrays.asList(
				Arrays.asList("Russia", "Canada", "USA", "China", "Brazil", "Australia", "India"),
				Arrays.asList(17098242, 9984670, 9826675, 9596961, 8514877, 7741220, 3287263));

		try (XSSFWorkbook wb = new XSSFWorkbook()) {

			barColumnChart(tableName + "_1", title, bottomAxis, leftAxis, seriesTitle, data, wb);
			barColumnChart(tableName + "_2", title, bottomAxis, leftAxis, seriesTitle, data, wb);
			barColumnChart(tableName + "_3", title, bottomAxis, leftAxis, seriesTitle, data, wb);

			// Write output to an excel file
			String filename = "1_bar-chart-top-seven-countries.xlsx";// "column-chart-top-seven-countries.xlsx";
			try (FileOutputStream fileOut = new FileOutputStream(filename)) {
				wb.write(fileOut);
			}

		}
		// barColumnChart();
	}

	public static void barColumnChart(String _tableName, String _title, String _bottomAxis, String _leftAxis,
			String _seriesTitle, List<List<Object>> _data, XSSFWorkbook _wb) throws FileNotFoundException, IOException {

		String sheetName = _tableName;// "CountryColumnChart";
		int noOfEntries = _data.get(0).size();

		XSSFSheet sheet = _wb.createSheet(sheetName);

		// Create row and put some cells in it. Rows and cells are 0 based.
		Row row = sheet.createRow((short) 0);

		Iterator<Object> i = _data.get(0).iterator();
		short index = 0;
		Cell cell;
		while (i.hasNext()) {
			cell = row.createCell(index++);
			cell.setCellValue((String) i.next());
		}

		row = sheet.createRow((short) 1);
		i = _data.get(1).iterator();
		index = 0;
		while (i.hasNext()) {
			cell = row.createCell(index++);
			cell.setCellValue((Double) i.next());
		}

		XSSFDrawing drawing = sheet.createDrawingPatriarch();
		XSSFClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, 0, 4, noOfEntries, 20);

		XSSFChart chart = drawing.createChart(anchor);
		chart.setTitleText(_title);
		chart.setTitleOverlay(false);

		XDDFChartLegend legend = chart.getOrAddLegend();
		legend.setPosition(LegendPosition.TOP_RIGHT);

		XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
		bottomAxis.setTitle(_bottomAxis);
		XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
		leftAxis.setTitle(_leftAxis);
		leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);

		XDDFDataSource<String> countries = XDDFDataSourcesFactory.fromStringCellRange(sheet,
				new CellRangeAddress(0, 0, 0, noOfEntries - 1));

		XDDFNumericalDataSource<Double> values = XDDFDataSourcesFactory.fromNumericCellRange(sheet,
				new CellRangeAddress(1, 1, 0, noOfEntries - 1));

		XDDFChartData data = chart.createData(ChartTypes.BAR, bottomAxis, leftAxis);
		XDDFChartData.Series series1 = data.addSeries(countries, values);
		series1.setTitle(_seriesTitle, null);
		data.setVaryColors(true);
		chart.plot(data);

		// in order to transform a bar chart into a column chart, you just need to
		// change the bar direction
		XDDFBarChartData bar = (XDDFBarChartData) data;
		bar.setBarDirection(BarDirection.COL);
		// bar.setBarDirection(BarDirection.COL);

	}

	public static void barColumnChart() throws FileNotFoundException, IOException {
		try (XSSFWorkbook wb = new XSSFWorkbook()) {

			String sheetName = "CountryBarChart";// "CountryColumnChart";

			XSSFSheet sheet = wb.createSheet(sheetName);

			// Create row and put some cells in it. Rows and cells are 0 based.
			Row row = sheet.createRow((short) 0);

			Cell cell = row.createCell((short) 0);
			cell.setCellValue("Russia");

			cell = row.createCell((short) 1);
			cell.setCellValue("Canada");

			cell = row.createCell((short) 2);
			cell.setCellValue("USA");

			cell = row.createCell((short) 3);
			cell.setCellValue("China");

			cell = row.createCell((short) 4);
			cell.setCellValue("Brazil");

			cell = row.createCell((short) 5);
			cell.setCellValue("Australia");

			cell = row.createCell((short) 6);
			cell.setCellValue("India");

			row = sheet.createRow((short) 1);

			cell = row.createCell((short) 0);
			cell.setCellValue(17098242);

			cell = row.createCell((short) 1);
			cell.setCellValue(9984670);

			cell = row.createCell((short) 2);
			cell.setCellValue(9826675);

			cell = row.createCell((short) 3);
			cell.setCellValue(9596961);

			cell = row.createCell((short) 4);
			cell.setCellValue(8514877);

			cell = row.createCell((short) 5);
			cell.setCellValue(7741220);

			cell = row.createCell((short) 6);
			cell.setCellValue(3287263);

			XSSFDrawing drawing = sheet.createDrawingPatriarch();
			XSSFClientAnchor anchor = drawing.createAnchor(0, 0, 0, 0, 0, 4, 7, 20);

			XSSFChart chart = drawing.createChart(anchor);
			chart.setTitleText("Area-wise Top Seven Countries");
			chart.setTitleOverlay(false);

			XDDFChartLegend legend = chart.getOrAddLegend();
			legend.setPosition(LegendPosition.TOP_RIGHT);

			XDDFCategoryAxis bottomAxis = chart.createCategoryAxis(AxisPosition.BOTTOM);
			bottomAxis.setTitle("Country");
			XDDFValueAxis leftAxis = chart.createValueAxis(AxisPosition.LEFT);
			leftAxis.setTitle("Area");
			leftAxis.setCrosses(AxisCrosses.AUTO_ZERO);

			XDDFDataSource<String> countries = XDDFDataSourcesFactory.fromStringCellRange(sheet,
					new CellRangeAddress(0, 0, 0, 6));

			XDDFNumericalDataSource<Double> values = XDDFDataSourcesFactory.fromNumericCellRange(sheet,
					new CellRangeAddress(1, 1, 0, 6));

			XDDFChartData data = chart.createData(ChartTypes.BAR, bottomAxis, leftAxis);
			XDDFChartData.Series series1 = data.addSeries(countries, values);
			series1.setTitle("Country", null);
			data.setVaryColors(true);
			chart.plot(data);

			// in order to transform a bar chart into a column chart, you just need to
			// change the bar direction
			XDDFBarChartData bar = (XDDFBarChartData) data;
			bar.setBarDirection(BarDirection.COL);
			// bar.setBarDirection(BarDirection.COL);

			// Write output to an excel file
			String filename = "bar-chart-top-seven-countries.xlsx";// "column-chart-top-seven-countries.xlsx";
			try (FileOutputStream fileOut = new FileOutputStream(filename)) {
				wb.write(fileOut);
			}
		}
	}

}
