import org.apache.spark.sql._
import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.config._
import com.stratio.datasource.mongodb.config.MongodbConfig._

import com.mongodb.spark._
import com.mongodb.spark.sql._




val corp_active_services = sc.textFile("hdfs://bigdata-1.monsterindia.noida:9000/user/bigdata/production/corp_active_services")

val corp_active_services_split = corp_active_services.map(_.split("\007"))



case class Datatest_corp_active_services(id: Int, name: String, type1: String, amount_usd: Int, amount_inr: Int, amount_hkd: Int,   amount_sgd: Int, description: String, enabled: String, add_date: String, sp_ind: String, sp_int: String, max_slab: Int, min_slab: Int, discount: Int, sortby: Int, validity: Int, free_type: String, show_on_site: String, subchannel_id: String)

val dataset_corp_active_services = corp_active_services_split.map(r => Datatest_corp_active_services(r(0).toInt, r(1).trim, r(2).trim, r(3).toInt, r(4).toInt, r(5).toInt, r(6).toInt, r(7).trim, r(8).trim, r(6).trim, r(10).trim, r(11).trim, r(12).toInt, r(13).toInt, r(14).toInt, r(15).toInt, r(16).toInt, r(17).trim, r(18).trim, r(19).trim)).toDF()

//dataset_corps.registerTempTable("corps")

//val skillsusers = sqlContext.sql("select id, scid, enabled, skill from skills")//.rdd


//val skills_data = skillsusers.map(s=> (s(0).toString.replaceAll(""",""","").toInt, s(1).toString, s(2).toString.replaceAll(""",""","").toInt, s(3).toString.replaceAll(""",""","").toInt))


//user_location_history_data.count()

//user_location_history_data.first()

//skills_data.saveToCassandra("bazooka", "skills", SomeColumns("id", "scid", "enabled", "skill"))

val saveConfig = MongodbConfigBuilder(Map(Host -> List("10.216.204.164:27017"), Database -> "bazooka", Collection ->"corp_active_services", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
//val writeConfig = saveConfig.build()

//Write the data to MongoDB - because of Spark's just-in-time execution, this actually triggers running the query to read from the 1-minute bars table in MongoDB and then writing to the 5-minute bars table in MongoDB
dataset_corp_active_services.saveToMongodb(saveConfig.build)




