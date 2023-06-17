import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{isnan, when, count, col}
import java.util.Properties
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcDialects, JdbcType}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import com.google.common.hash.Hashing
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._


object ExtractBus {
        def main(args: Array[String]) {


        val spark = SparkSession.builder.appName("ETL").master("local[4]").getOrCreate()
                import spark.implicits._
                import java.util.Properties


                val jdbcUrl = "jdbc:postgresql://stendhal:5432/tpid2020"
        val table = "yelp.review"
        val user = ""
        val password = ""

        // Charger une table PostgreSQL review dans un DataFrame et l'extraction de l'année et le trimestre
        var review = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("user", user)
        .option("password", password)
        .option("dbtable","""(select business_id, user_id, extract(year from date) as year,
            extract(quarter from date) as quarter,
            stars, spark_partition
            from
            yelp.review) as stt""").option("partitionColumn","spark_partition")
            .option("lowerBound","0")
            .option("upperBound","99")
            .option("numPartitions","10")
        .load()


        //on a crée cette dataframe pour voir le nombre de review d'un utilisateur lors de son année élite
        var user_date_rev = review.select("user_id","year")

        //chargement de la table elite
        var elite = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("user", user)
        .option("password", password)
        .option("dbtable","yelp.elite").load()

        //compter le nombre de reviews de l'utilisateur lors de son année d'élite
        var user_nbReview_year_elite = user_date_rev.join(elite, Seq("user_id","year"))
        user_nbReview_year_elite = user_nbReview_year_elite.groupBy("user_id").agg(
            count("*").as("nb_de_reviews_elite"),
            countDistinct("year").as("nb_years_elite")
        )

        //l'attribut year était en type float
        //user_nbReview_year_elite = user_nbReview_year_elite.withColumn("year_elite",col("year").cast("int").cast("string")).drop("year")


        //calcul du nombre d'évaluation pour chaque utilisateur ainsi que la valeur review moyenne
        var bus_fait = review.groupBy("business_id","year","quarter").agg(
            avg("stars").as("stars"),
            count(lit(2)).alias("nombre de review")
        )
        
        //création de table dimension utilisateur pendant l'étape du chargement        
        var user_dim = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("user", user)
        .option("password", password)
        .option("query","""select user_id, name, yelping_since from yelp.user""")
        .load()

        user_dim = user_dim.na.drop(Seq("name","yelping_since"))
        //suppression des utilisateurs qui ont de noms non alpha-numérique et qui ont moins de deux caractères
        //user_dim = user_dim.filter(col("name").rlike("^[a-zA-Z0-9]*$"))
        //val w = Window.orderBy("yelping_since")
        //user_dim = user_dim.withColumn("since_id",row_number().over(w))

        
        var yelping_since = user_dim.select("yelping_since").dropDuplicates()
        yelping_since = yelping_since.withColumn("since_id", monotonically_increasing_id)
        //user_dim = user_dim.drop("yelping_since")

        yelping_since = yelping_since.select(col("since_id"),
                        col("yelping_since"),
                        year(col("yelping_since")).as("year"),
                        month(col("yelping_since")).as("month"),
                        dayofmonth(col("yelping_since")).as("day")
                        )

        user_dim = user_dim.join(yelping_since, Seq("yelping_since"))
        user_dim = user_dim.drop("yelping_since")
        

        //création de la table de fait user
        var user_fait = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("user", user)
        .option("password", password)
        .option("query","""select r.user_id, u.useful  ,fans,
            count(*) review_count , sum(case when r.stars>3 then 1 else 0 end) as pos_rating,
            sum(case when r.stars<3 then 1 else 0 end) as neg_rating,
            sum(case when r.stars=3 then 1 else 0 end) as neut_rating  
            from yelp."user" u , yelp.review r
            where u.user_id = r.user_id group by r.user_id, u.useful, fans
            """)
        .load()

        //nettoyage de la table de fait

        user_fait = user_fait.na.drop(Seq("user_id"))

        user_fait = user_fait.join(user_dim.select("user_id","since_id"), Seq("user_id"))
        //jointure entre user fait and user_nbReview_year_elite pour obtenir la measure nbReview_year_elite
        user_fait = user_fait.join(user_nbReview_year_elite,Seq("user_id"))
        var user_friend = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("user", user)
        .option("password", password)
        .option("dbtable","""(select u.user_id as user_id, spark_partition from
            yelp."user" u , yelp.friend f  where u.user_id  = f.user_id
           ) as subq""")
            .option("partitionColumn","spark_partition")
            .option("lowerBound","0")
            .option("upperBound","99")
            .option("numPartitions","10")
        .load()


                val busFile = "data/yelp_academic_dataset_business.json"                
                // Chargement du fichier JSON
                var business = spark.read.json(busFile).cache()
        
        //création de dimension zone dimension
        var zone = business.select("business_id","city","postal_code","latitude","longitude")
        //suppression des lignes dupliquées
        zone = zone.dropDuplicates()
        //création d'une colonne zone_id
        zone = zone.withColumn("zone_id", monotonically_increasing_id)

        //selection des attributs nécessaires pour la création de la table de fait
        var neededInfo_bus = business.select("business_id", "name", "address", "review_count")
        //selection des attributs nécessaires pour la création de la table de dimension catégorie
        var busCeteg = business.select("business_id","categories")


        //split l'attribut categorie pour avoir tous les categories pour chaque business
        var bus_cat =  busCeteg.withColumn("categorie", explode(org.apache.spark.sql.functions.split(
            col("categories"), ", ")))

        // function pour l'encodage de categorie
        val hashFunc = udf((s: String) => Hashing.murmur3_32().hashString(s, java.nio.charset.StandardCharsets.UTF_8).asInt.abs)
        bus_cat = bus_cat.withColumn( "categorie_id", hashFunc(col("categorie") ))
        
        var cat_enc = bus_cat.select("categorie","categorie_id").dropDuplicates()
        bus_cat = bus_cat.drop("categories","categorie")
        
        
        var checkin_url = "data/yelp_academic_dataset_checkin.json"
        var checkin_business = spark.read.json(checkin_url).cache()
        var needed_checkinInfo = checkin_business.select("business_id","date")
        
        //function permet de calculer le nombre de visite pour chaque business
        val nombre_de_visite = udf((arg: String) => { arg.split(" ").length})
        needed_checkinInfo = needed_checkinInfo.withColumn("nombre_de_visite", nombre_de_visite(col("date")))

        //split d'attribut date pour obtenir toutes les dates où le business a été visité
        val date_visite = udf((arg:String) => {arg.split(" ")})
        var bus_vis = needed_checkinInfo.withColumn("date_vis",explode(date_visite(col("date")))).drop("date","nombre_de_visite")
        needed_checkinInfo = needed_checkinInfo.drop("date")

        //merger les données de needed_checkinfo et neededInfo
        var merged_business = neededInfo_bus.join(needed_checkinInfo, Seq("business_id") )
        var bus_dim = merged_business.dropDuplicates()
        //netteoyage de la table de valeurs nulles et de vide
        bus_dim = bus_dim.na.drop(Seq("name","business_id"))
        bus_dim  = bus_dim.filter(col("name") =!= "")
              
        //merger la table review avec zone pour construire la table de fait business
        bus_fait = bus_fait.join(zone,Seq("business_id")).drop("city","postal_code")
        bus_fait = bus_fait.na.drop(Seq("business_id"))
        
        //suppression de la colonne business_id de la table zone
        zone = zone.drop("business_id")

        Class.forName("oracle.jdbc.driver.OracleDriver")
        var url1 = "jdbc:oracle:thin:@stendhal:1521:enss2022"
        import java.util.Properties
        var connectionProperties1 = new Properties()
        connectionProperties1.setProperty("driver", "oracle.jdbc.OracleDriver")
        connectionProperties1.setProperty("user", "ab701649")
        connectionProperties1.setProperty("password","ab701649")
        val dialect = new OracleDialect
        JdbcDialects.registerDialect(dialect)

      
        // bus_dim.write.mode(SaveMode.Overwrite).jdbc(url1,"bus_dim",connectionProperties1)
        // // var nanCounts = bus_dim.select(bus_dim.columns.map(c => count(when(isnan(col(c)) || col(c).isNull, c)).alias(c)): _*)
        // // print("bus_dim",nanCounts.show)
        
        // zone.write.mode(SaveMode.Overwrite).jdbc(url1,"zone_dim",connectionProperties1)
        // // nanCounts = zone.select(zone.columns.map(c => count(when(isnan(col(c)) || col(c).isNull, c)).alias(c)): _*)
        // // print("zone",nanCounts.show)
        // bus_cat.write.mode(SaveMode.Overwrite).jdbc(url1,"bus_cat_dim",connectionProperties1)
        // // nanCounts = bus_cat.select(bus_cat.columns.map(c => count(when(isnan(col(c)) || col(c).isNull, c)).alias(c)): _*)
        // // print("bus_cat",nanCounts.show)
        // cat_enc.write.mode(SaveMode.Overwrite).jdbc(url1,"categorie_dim",connectionProperties1)
        // // nanCounts = cat_enc.select(cat_enc.columns.map(c => count(when(isnan(col(c)) || col(c).isNull, c)).alias(c)): _*)
        // // print("cat_enc",nanCounts.show)
        // user_dim.write.mode(SaveMode.Overwrite).jdbc(url1,"user_dim",connectionProperties1)
        var nanCounts = user_dim.select(user_dim.columns.map(c => count(when(isnan(col(c)) || col(c).isNull, c)).alias(c)): _*)
        print("user_dim",nanCounts.show)
        // user_fait.write.mode(SaveMode.Overwrite).jdbc(url1,"user_fait",connectionProperties1)
        nanCounts = user_fait.select(user_fait.columns.map(c => count(when(isnan(col(c)) || col(c).isNull, c)).alias(c)): _*)
        print("user_fait",nanCounts.show)
        // bus_fait.write.mode(SaveMode.Overwrite).jdbc(url1,"bus_fait_part1",connectionProperties1)
        // // nanCounts = bus_fait.select(bus_fait.columns.map(c => count(when(isnan(col(c)) || col(c).isNull, c)).alias(c)): _*)
        // // print("bus_fait",nanCounts.show)
        yelping_since.show()
        user_dim.show()
        user_fait.show()
        spark.stop()

 
    }
}