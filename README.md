# pyspark-airflow-aws


### Data source

- I94 Immigration Data: This data comes from the US National Tourism and Trade Office [Source](https://travel.trade.gov/research/reports/i94/historical/2016.html). This data records immigration records partitioned by month of every year.

|         | cicid   | i94yr | i94mon | i94cit | i94res | i94port | arrdate | i94mode | i94addr | depdate | i94bir | i94visa | count | dtadfile | visapost | occup | entdepa | entdepd | entdepu | matflag | biryear | dtaddto  | gender | insnum | airline | admnum   | fltno | visatype |
|---------|---------|-------|--------|--------|--------|---------|---------|---------|---------|---------|--------|---------|-------|----------|----------|-------|---------|---------|---------|---------|---------|----------|--------|--------|---------|----------|-------|----------|
| 2027561 | 4084316 | 2016  | 4      | 209    | 209    | HHW     | 20566   | 1       | HI      | 20573   | 61     | 2       | 1     | 20160422 |          |       | G       | O       |         | M       | 1955    | 7202016  | F      |        | JL      | 5.66E+10 | 782   | WT       |
| 2171295 | 4422636 | 2016  | 4      | 582    | 582    | MCA     | 20567   | 1       | TX      | 20568   | 26     | 2       | 1     | 20160423 | MTR      |       | G       | R       |         | M       | 1990    | 10222016 | M      |        | *GA     | 9.44E+10 | XBLNG | B2       |
| 589494  | 1195600 | 2016  | 4      | 148    | 112    | OGG     | 20551   | 1       | FL      | 20571   | 76     | 2       | 1     | 20160407 |          |       | G       | O       |         | M       | 1940    | 7052016  | M      |        | LH      | 5.58E+10 | 464   | WT       |
| 2631158 | 5291768 | 2016  | 4      | 297    | 297    | LOS     | 20572   | 1       | CA      | 20581   | 25     | 2       | 1     | 20160428 | DOH      |       | G       | O       |         | M       | 1991    | 10272016 | M      |        | QR      | 9.48E+10 | 739   | B2       |
| 3032257 | 985523  | 2016  | 4      | 111    | 111    | CHM     | 20550   | 3       | NY      | 20553   | 19     | 2       | 1     | 20160406 |          |       | Z       | K       |         | M       | 1997    | 7042016  | F      |        |         | 4.23E+10 | LAND  | WT       |
