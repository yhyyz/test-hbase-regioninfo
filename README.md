### 从S3上读取HBase regioninfo 文件，测试文件正确性

```shell
# 可以不用编译，直接idea执行就可以
mvn clean compile package
java -jar test-hbase-regioninfo-1.0-SNAPSHOT.jar s3n://*****/.regioninfo  accessid  accesskey
```