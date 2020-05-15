#Test join condition :

Test case file : [/my/excel_embed_test/Test1.xls] (- "#path")
[ ](- "setFileName(#path)")

With [my.student] (- "#tname")
[-](- "cx:embed=printData(#tname)")

With [my.grades] (- "#tname1")
[-](- "cx:embed=printData(#tname1)")

When we join student and grades table based on percentage value[ ](- "#result1 = runTest1(#excelPath_Test1)"),

Result should be as shown in [verify] (- "#tname2") tab of excel.
[-](- "cx:embed=printData(#tname2)")

The test case is : [passed](- "?=#result1.outcome")

Details : [ ](- "cx:embed=#result1.details")
