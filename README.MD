You can use following maven commands to run your application.
#Run Java Program
```
mvn exec:java -Dexec.mainClass="com.jhh.Main"  
mvn exec:java -Dexec.mainClass="akka.Main" -Dexec.args="com.jhh.HelloWorld" 
```

#Run Scala Program
```
mvn scala:run -DmainClass=com.jhh.App -DaddArgs="arg1|arg2|arg3"
```

#Assembly all project into a single jar file 
```
mvn clean compile assembly:single

or

mvn clean compile package
```
After the above command, there are two jar files would be generated: 
1. a jar file with all depandent packages
2. a single jar file only containing the project source code

#Change main and premain class file in pom file
You need to revise the pom file to specify a new main/premain 
class entry if you want to a new one in your code. The default
value is "org.ucf.spark.ScalaApp"

There are two properties in the pom file
1. **project.main.class**:  Set the main entry  of all project 
2. **project.main.class**:  Set the premain entry of Java instrumenttion


# Execute jar file
```
java -cp {all needed depandency package } -jar {the target jar file} 
```
For example 

```
$ pwd
/Users/Bing/Documents/workspace/test
$ ls
README.MD	pom.xml		src		target		test.iml
$ java -jar target/test-1.0-SNAPSHOT-jar-with-dependencies.jar 
Hello World from Scala
```

In the pom file,  "{package}.ScalaApp" is set as the main entry class of the project. 
If you want to switch to "{package}.JavaApp", you just modify pom file