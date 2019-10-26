## Sql to DataFrame Generator 

Supported Sql: SAS and Hive

### Aim : 

To convert most of the sql queries to Spark DataFrames(scala) and reduce copy paste work of developers while doing the same.

### Motivation :

Spark DataFrames provide high performance and reduce cost. So, I converted Many SQL(Hive and SAS) scripts to spark DataFrames.
During conversion of sql to DataFrames I observed a similar pattern in every sql conversion.
I created this project which converts SQL to DataFrame.

It converts complex nested SQL, arithmetic expressions and nested functions, to spark DataFrame's.
Sample Example is provided below.


### Why to convert to DataFrames when we can run SQL in spark mode

Advantages of DataFrame over sql:-

* Advantage to write UDF and UDAF using FP's of scala.

* Better type safe than sql.

* Using functional programming API we can define filters or Select statement at runtime.

* Splitting complex sql to small data frames used for optimizations.

* Decrease load over meta store.

### Sample Examples:

#### Input SAS sql

```
CREATE TABLE STUDENT_TEMP AS
        SELECT
         	DISTINCT student_id AS id,
              weight||'_'||(CASE
           WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
           WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
           WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
           ELSE 'VERY HIGH'
           END) AS NEWWEIGHT
         FROM
           STUDENT;
           
```

#### output dataframe

```
val STUDENT_TEMP = student. 
 select($"student_id".as("id"),concat($"weight",lit("_"),
    when($"weight".between(lit(0),lit(50)),lit("low")).
     otherwise(when($"weight".between(lit(51),lit(70)),lit("medium")).
     otherwise(when($"weight".between(lit(71),lit(100)),lit("high")).
     otherwise(lit("very high"))))).as("newweight")).
  distinct 
```

#### Input Nested SAS sql

```
CREATE TABLE STUDENT_TEMP AS
        SELECT
         	DISTINCT student_id AS id,
              weight||'_'||(CASE
           WHEN WEIGHT BETWEEN 0 AND 50 THEN 'LOW'
           WHEN WEIGHT BETWEEN 51 AND 70 THEN 'MEDIUM'
           WHEN WEIGHT BETWEEN 71 AND 100 THEN 'HIGH'
           ELSE 'VERY HIGH'
           END) AS NEWWEIGHT
         FROM
           (SELECT * FROM STUDENT WHERE student_id is not null);
           
```

#### output dataframes

```
val STUDENT_TEMP_a = student.
filter($"STUDENT_ID".isNotNull). 
 select($"*") 
 
val STUDENT_TEMP = STUDENT_TEMP_a.as("a"). 
 select($"student_id".as("id"),concat($"weight",lit("_"),when($"weight".between(lit(0),lit(50)),lit("low")).
    otherwise(when($"weight".between(lit(51),lit(70)),lit("medium")).
    otherwise(when($"weight".between(lit(71),lit(100)),lit("high")).
    otherwise(lit("very high"))))).as("newweight")).
 distinct 
 
```

### Flow of the code

![Cat](https://github.com/mvamsichaitanya/sql-to-dataframe-generator/blob/master/src/main/resources/images/sql-to-dataframe-generator.jpg)


### How to use:

#### first procedure

1. Clone the code from git


2. Edit the properties file in Config.scala(No extra config required to differentiate hive or sas)(configs kept in scala file as user will be Data Engineer)


3. Edit SasSqlToSparkSql.scala and SqltoDf.scala to iterate each sql according to your input.


4. Run the MainRun.scala class

#### second procedure

1. Add jar to the class path.

2. Instantiate io.github.mvamsichaitanya.codeconversion.sqltodataframe.DataFrame by passing select statement to it.

3. toString method will return DataFrame code

### Note

1. No Create table statements should be present as below in the input file.

```
Create table table_1(column_1 int,

                      column_2 bigint,
                      
                        column_3 string);

```

2. No Union all Queries

3. It will not convert to DataFrames with 100% accuracy. Developer should validate with SQL after conversion.



### LICENSE

[MIT](https://github.com/mvamsichaitanya/sql-to-dataframe-generator/blob/master/LICENSE)
