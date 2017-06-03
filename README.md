**This application uses Spark 2.1.0 version which is marked as 'provided' inside ```build.sbt``` file.**

### Run command usage

```
spark-submit --class com.fp.GumGumJoiner --master local[*] <jar> <adevents> <assets> <output>
```

### Run command with prepared file paths

```
spark-submit 
--class com.fp.GumGumJoiner \
--master local[*] \
target/scala-2.11/gumgum-challenge-assembly-1.0.jar \
src/main/resources/adevents \
src/main/resources/assets \
src/main/resources/output
```

### Artifacts
1. jar with classes => ```target/scala-2.11/*```
2. source code => ```src/main/scala/*```
3. instructions => ```README.md```
4. output file => ```src/main/resources/myoutput/part-00000-e2377d2b-fff6-4d6b-a721-72355bb4562c.txt```