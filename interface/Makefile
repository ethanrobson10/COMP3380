
build: HockeyDBInterface.class

HockeyDBInterface.class: HockeyDBInterface.java
	javac HockeyDBInterface.java

run: HockeyDBInterface.class
	java -cp .:mssql-jdbc-11.2.0.jre11.jar HockeyDBInterface

clean:
	rm HockeyDBInterface.class
	rm HockeyDB.class
	rm TablePrinter.class
