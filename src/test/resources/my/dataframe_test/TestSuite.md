# Student's Grader Application 
Testing all referentially transparent methods in Grader application.

### [Join Student and Grade Tables ](- "Test1")

Test case file : [/my/dataframe_test/Test1.xls] (- "#excelPath_Test1")

When we join student and grades table based on percentage value[ ](- "#result1 = runTest1(#excelPath_Test1)"),

The test case is : [passed](- "?=#result1.outcome")

Details : [ ](- "c:echo=#result1.details")


### [Select above 90 students ](- "Test2")

Test case file : [/my/dataframe_test/Test2.xls] (- "#excelPath_Test2")

When we select only above 90 students [ ](- "#result2 = runTest2(#excelPath_Test2)"),

The test case is : [passed](- "?=#result2.outcome")

Details : [ ](- "c:echo=#result2.details")

[-](- "cx:embed=getData1()")
