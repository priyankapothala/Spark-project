
# Finding states in US with stable temperature

The goal of the project is to find out which states in the US
have the most stable temperature.

## Dependencies

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install pyspark.

```bash
pip install pyspark
```
## Executing the script
```bash
spark-submit main.py <locations_path> <recordings_path> <output_path>
```
##### Ex: spark-submit main.py "/tmp/usr/locations" "/tmp/usr/recordings" "/tmp/usr/output"

## A note on the values for precipitation

A value of 99.99 means that there is no data. There is a letter at the end of the recordings. Here is what the letters mean (Basically the letter tells you how long the precipitation was accumulated before recording).

- A - 6 hours worth of precipitation

- B - 12 hours worth of precipitation

- C - 18 hours worth of precipitation

- D - 24 hours worth of precipitation

- E - 12 hours (slightly different from B but the same for this project).

- F - 24 hours (slightly different from D but the same for this project).

- G - 24 hours (slightly different from D but the same for this project).

- H - station recorded a 0 for the day (although there was some recorded instance of precipitation).

- I - station recorded a 0 for the day (and there was NO recorded instance of precipitation).

## Summary

- First, we read the locations file from the locations folder and ignore the records with missing STATE values and where CTRY is not "US". 
- Then we read the recordings folder, filter the multiple headers and extract the necessary columns. The two dataframes are converted into views. Precipitation values are extrapolated to 24hours worth of precipitation
- These two views are then joined on *USAF* column and the average precipitation and temperature values are calculated for each state by grouping on STATE and MONTH. Precipitation values with 99.99 are ignored (considered as null) while finding the average
- The highest and lowest average temperature for each STATE are then obtained by using RANK and PARTITION BY
- The difference between the highest and lowest average temperatures is calculated for each state and
and the output is written to a csv file in increasing order of difference

## Detailed Description

### 1. Creating  a Spark Session
First, we create a **SparkSession** and set the master as **local** to run locally.

```python
spark = SparkSession.builder.master("local").appName('CS235').getOrCreate()
```
### 2. Reading locations
- The locations file is read from the locations folder into a dataframe.
- All the records with missing *STATE* value and where *CTRY* is not *'US'* are ignored.  
- A view **locations_vw** is created from the resulting dataframe.

### 3. Reading recordings
- The recordings are read from the recordings folder into a dataframe and the headers are filtered by checking if a row contains **STN--**.  
- Multiple spaces in the row is replaced by a single space so that the rows can be split easily into columns.  
- *USAF*, *WBAN*, *TEMP*, *MONTH*, *PRCP* fields are extracted from the *value* column by splitting using the single space delimiter.  
- The last letter from *PRCP* value is extracted and the value of the precipitation is calculated using  a user defined function **calc_prec**.
- For example if the last letter of *PRCP* is A, then the value is multiplied by 4 to extrapolate 24 hours worth of precipitation. The values are calculated for other letters similarly
- *MONTH* is extracted from *YEARMODA* using *substr*.  
- The *value* column is finally dropped and a view **recordings_vw** is created from the dataframe.

### 4. Finding the average temperature and precipitation for each month
- The two views **locations_vw** and **readings_vw** are then joined on *USAF* column and grouped by STATE, MONTH to calculate the average temperature and average precipitation for each month. - The result is then stored in **state_month_vw** view. 
- Precipitation values with 99.99 are ignored(considered as null in calculation).

### 5. Finding months with the highest and lowest averages for each state
- The months with the highest and lowest average temperature for each state are obtained from *state_month_vw* by ranking the average temperature within a state partition ordered by the average temperature using *RANK* and *PARTITION BY*.
- We then filter and obtain the highest and lowest average temperature for each state.
- The result is stored in a dataframe *state_month_avg_df*.

### 6. Getting month name
- A user defined function **get_month** has been defined to get the name of the month using month number

### 7. Outputting the Result
- The highest and the lowest average temperatures and their corresponding months are collected into a list using aggregation method *collect_list* by grouping on *STATE*.
- Average precipitation for two months is also calculated and rounded to two decimals. 
- The highest and the lowest average temperatures are rounded to two decimals and their corresponding months are concatented.
- The difference between the highest and lowest average temperatures is calculated
- The values of state, highest average temperature and month, lowest average temperature and month, difference and average precipitation are output to a single csv file in the output folder in increasing order of difference using *coalesce* to merge the partitions.


