# Miguel's Assingment For Asos

The assingment is, based on three different files sources, expecting to create different CSV and parquet files:

  - CSV containing average rating per movie and how many different users rated it
  - CSV with the list of different genders of movies and how many movies are in each of them
  - Parquet file incluiding the top 100 movies in terms of average rating - includes movie ID, movie title, average ranking and ranking (having 1 as the highest ranking)

# Pre requirements
If cloned, code can be launched without any pre requirements.
Otherwise, it is required to place the MovieLens files into the main resources directory so the process can access them.

# Launch the process
First, make sure that the directory to be used as sink directory has the rights permissions to do so. If required, it might be needed to launch the following command with sudo permission.
```sh
$ chmod -R 755 /path/to/sink/directory
```

As an Apache Spark process, it can be trigger with the `spark-submit` command.
For this process, it has been used the Spark version `2.3.4`.
In order to make the process easier to use, it has been declared one mandatory parameter: the directory mentioned above.
As so, it is required to package the whole code into a single .jar file:

```sh
$ cd /path/to/pom.xml/file
$ mvn -U clean package
```

With the previous maven command, the .jar file should have been created.
Next step is just running the process with the `spark-submit` command.

```sh
$ spark-submit --class org.asos.interview.AsosInterviewMain --master local /path/to/jar/directory/ASOS-0.0.1-SNAPSHOT.jar /path/to/sink/directory
```

There are two ways for validating the process:

  - Validating the CSV files created so the expected results are there
  - The parquet file should be easy to read - and print in the terminal - with an easy command:
 ```sh
$ spark-shell
 scala> spark.read.parquet("/path/to/sink/directory/ranking_movies_ratings_filename.parquet/name-of-parquet-file.parquet").show(false)
```
  - The result should be as follows:
![Expected Final Result](https://github.com/morbvel/AsosAsingment/blob/master/result.png)
