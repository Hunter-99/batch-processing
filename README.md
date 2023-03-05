
`main.py` script output: 
```bash
➜  batch-processing git:(master) ✗ python -m main
23/03/05 15:42:18 WARN Utils: Your hostname, laptop resolves to a loopback address: 127.0.1.1; using 192.168.1.104 instead (on interface wlo1)
23/03/05 15:42:18 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
23/03/05 15:42:19 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
23/03/05 15:42:19 WARN Utils: Service 'SparkUI' could not bind on port 4040. Attempting port 4041.
23/03/05 15:42:19 WARN Utils: Service 'SparkUI' could not bind on port 4041. Attempting port 4042.
Csv partition file size in MegaBytes is 23.662659645080566
Csv partition file size in MegaBytes is 23.66229248046875
Csv partition file size in MegaBytes is 23.654913902282715
Csv partition file size in MegaBytes is 23.662592887878418
Csv partition file size in MegaBytes is 23.659021377563477
Csv partition file size in MegaBytes is 23.653108596801758
Csv partition file size in MegaBytes is 23.64396095275879
Csv partition file size in MegaBytes is 23.655160903930664
Csv partition file size in MegaBytes is 23.64557456970215
Csv partition file size in MegaBytes is 23.655685424804688
Csv partition file size in MegaBytes is 23.648698806762695
Csv partition file size in MegaBytes is 23.65985870361328
452470                                                                          
+----------------------------------------------------------------------------------------------------------------------------+
|max(((unix_timestamp(dropoff_datetime, yyyy-MM-dd HH:mm:ss) - unix_timestamp(pickup_datetime, yyyy-MM-dd HH:mm:ss)) / 3600))|
+----------------------------------------------------------------------------------------------------------------------------+
|66.8788888888889                                                                                                            |
+----------------------------------------------------------------------------------------------------------------------------+

+------------+-------------------------+------+                                 
|PULocationID|Zone                     |count |
+------------+-------------------------+------+
|61          |Crown Heights North      |231279|
|79          |East Village             |221244|
|132         |JFK Airport              |188867|
|37          |Bushwick South           |187929|
|76          |East New York            |186780|
|231         |TriBeCa/Civic Center     |164344|
|138         |LaGuardia Airport        |161596|
|234         |Union Sq                 |158937|
|249         |West Village             |154698|
|7           |Astoria                  |152493|
|148         |Lower East Side          |151020|
|68          |East Chelsea             |147673|
|42          |Central Harlem North     |146402|
|255         |Williamsburg (North Side)|143683|
|181         |Park Slope               |143594|
|225         |Stuyvesant Heights       |141427|
|48          |Clinton East             |139611|
|246         |West Chelsea/Hudson Yards|139431|
|17          |Bedford                  |138428|
|170         |Murray Hill              |137879|
+------------+-------------------------+------+
```