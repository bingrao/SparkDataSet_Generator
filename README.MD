## Sql to DataFrame Generator 

To convert most of the sql queries to Spark DataFrames(scala) and reduce copy paste work of developers while doing the same.

### Motivation :

Spark DataFrames provide high performance and reduce cost. So, I converted Many SQL scripts to spark DataFrames. It converts complex nested SQL, arithmetic expressions and nested functions, to spark DataFrame's.
Sample Example is provided below.

### Why to convert to DataFrames when we can run SQL in spark mode

Advantages of DataFrame over sql:-

* Advantage to write UDF and UDAF using FP's of scala.

* Better type safe than sql.

* Using functional programming API we can define filters or Select statement at runtime.

* Splitting complex sql to small data frames used for optimizations.

* Decrease load over meta store.

### Sample Examples:

#### Input sql

```
SELECT count(*) FROM head WHERE age  >  56           
```

#### output dataframe

```
head.where(col("age") > 56).agg(count("*"))
```

Please refer to the more case study in File [case examples](src/main/resources/case_example.log)

### Notes
This project is still in early development, if there is any problem, please free to let me know.

Please refer to the [unSupport SQL](src/main/resources/unsupportSQL.log) file to see the unsupport sql.
