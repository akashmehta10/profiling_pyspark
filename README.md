# Data Profiling/Data Quality (Pyspark)
Data profiling is the process of examining the data available from an existing information source (e.g. a database or a file) and collecting statistics or informative summaries about that data. The profiling utility provides following analysis:

- Percentage of NULL/Empty values for columns
- Overall summary, average, standard deviation, percentiles for numeric columns
- Unique/Distinct values for certain key columns
- Distribution counts for aggregated columns
- Percentage of data quality issues for columns 
 
 Full Description: https://medium.com/@akashmehta10/data-profiling-data-quality-in-spark-big-data-with-pluggable-code-a75fbff9865
 
NULL/Empty Percentage for Columns

|Column        |NullPercentage|
|--------------|--------------|
|gender        |0.0%          |
|age           |20.0%         |
|education     |0.0%          |
|occupation    |0.0%          |
|income        |0.0%          |
|street_address|0.0%          |
|city          |17.5%         |
|state         |5.0%          |

Summary for Numeric Columns

|summary|age               |income          |
|-------|------------------|----------------|
|count  |32                |40              |
|mean   |26.3125           |117556.55       |
|stddev |26.182285934849023|47448.1794122377|
|min    |-21               |40433           |
|25%    |20                |74585           |
|50%    |25                |116989          |
|75%    |28                |156289          |
|max    |155               |199284          |

Distinct Counts for Aggregate Columns

|Column    |DistinctCount|
|----------|-------------|
|occupation|27           |
|state     |26           |

Distribution Count for Aggregate Columns

======== Distribution for - occupation ========

|occupation    |count|
|--------------|-----|
|Lawer         |4    |
|Singer        |3    |
|Astronomer    |3    |
|Social Worker |3    |
|Dancer        |2    |
|Police Officer|2    |
|Historian     |2    |
|Actor         |2    |
|Florist       |1    |
|Photographer  |1    |

======== Distribution for - state ========

|state         |count|
|--------------|-----|
|Texas         |6    |
|California    |5    |
|Arizona       |2    |
|Colorado      |2    |
|null          |2    |
|North Carolina|2    |
|Tennessee     |2    |
|Pennsylvania  |1    |
|Oklahoma      |1    |
|Nevada        |1    |

Data Quality Issue Percentage for Columns

|Column    |MismatchPercentage|
|----------|------------------|
|gender    |5.0%              |
|age       |7.5%              |
|first_name|0.0%              |




## Configuration Steps

### Step 1
Include profile_lib.py library and adjust profile_driver.py based on requirements.

### Step 2
spark-submit profile_driver.py --py-files profiling-library/profile_lib.py