
build: DBExample.class

DBExample.class: DBExample.java
	javac DBExample.java

run: DBExample.class
	java -cp .:mssql-jdbc-11.2.0.jre11.jar DBExample

clean:
	rm DBExample.class
	rm MyDatabase.class
