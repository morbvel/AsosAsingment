# Miguel's Assingment For Asos

The assingment is, based on three different files sources, expecting to create different CSV and parquet files:

  - CSV containing average rating per movie and how many different users rated it
  - CSV with the list of different genders of movies and how many movies are in each of them
  - Parquet file incluiding the top 100 movies in terms of average rating - includes movie ID, movie title, average ranking and ranking (having 1 as the highest ranking)

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
+-------+-------------------------------------------------------------------+-------------+-------+
|movieId|movieTitle                                                         |averageRating|ranking|
+-------+-------------------------------------------------------------------+-------------+-------+
|787    |Gate of Heavenly Peace, The (1995)                                 |5.0          |1      |
|3382   |Song of Freedom (1936)                                             |5.0          |2      |
|3607   |One Little Indian (1973)                                           |5.0          |3      |
|989    |Schlafes Bruder (Brother of Sleep) (1995)                          |5.0          |4      |
|3656   |Lured (1947)                                                       |5.0          |5      |
|3881   |Bittersweet Motel (2000)                                           |5.0          |6      |
|1830   |Follow the Bitch (1998)                                            |5.0          |7      |
|3280   |Baby, The (1973)                                                   |5.0          |8      |
|3233   |Smashing Time (1967)                                               |5.0          |9      |
|3172   |Ulysses (Ulisse) (1954)                                            |5.0          |10     |
|3245   |I Am Cuba (Soy Cuba/Ya Kuba) (1964)                                |4.8          |11     |
|53     |Lamerica (1994)                                                    |4.75         |12     |
|2503   |Apple, The (Sib) (1998)                                            |4.67         |13     |
|2905   |Sanjuro (1962)                                                     |4.61         |14     |
|2019   |Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954)|4.56         |15     |
|318    |Shawshank Redemption, The (1994)                                   |4.55         |16     |
|858    |Godfather, The (1972)                                              |4.52         |17     |
|50     |Usual Suspects, The (1995)                                         |4.52         |18     |
|745    |Close Shave, A (1995)                                              |4.52         |19     |
|527    |Schindler's List (1993)                                            |4.51         |20     |
+-------+-------------------------------------------------------------------+-------------+-------+
