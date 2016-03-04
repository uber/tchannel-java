.PHONY : release

release:	
	@echo "please make sure you are using java 7."
	@read -p "Press any key to continue, or press Control+C to cancel. " x;
	mvn -Dbuild=release release:clean release:prepare
	mvn -Dbuild=release release:perform
