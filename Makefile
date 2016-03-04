.PHONY : release

release:	
	mvn -Dbuild=release release:clean release:prepare
	mvn -Dbuild=release release:perform
