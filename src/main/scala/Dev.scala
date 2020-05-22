import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Dev {

  def myfun(spark:SparkSession):DataFrame={
    import spark.implicits._

    val bt_plan_benefitDF=spark.sql("select * from phoenix_uat_db.bt_plan_benefit")
      .withColumn("BNFTTYPE",when(trim($"benefit").like("%C%"),"HCB")
      .otherwise(null)).where($"BNFTTYPE".isNotNull)
        .withColumn("Flag",when(trim($"benefit").like("00C") or trim($"benefit").like("00C01"),"ICU")
        .otherwise(when(trim($"benefit").rlike("00C02"),"NON-ICU")))
        .where($"BNFTTYPE".isNotNull).withColumn("plan_id",$"plan_id")
      .withColumn("item_no",$"item_no")

    val bt_plan_limitDF = spark.sql("SELECT * FROM phoenix_uat_db.bt_plan_limit")
      .withColumn("plan_id",$"plan_id").withColumn("item_no",$"item_no")

    val win1 = Window.partitionBy("bnfttype","plan_id","Flag")
    val win2 = Window.partitionBy("plan_id")

   var disDF = bt_plan_benefitDF.join(bt_plan_limitDF,Seq("plan_id","item_no"),"inner")
        .withColumn("hcb_amt",when($"max_coverage_period".isNotNull and $"max_coverage_period".rlike("30D|1M"),$"amt_per_day"*30)
        .otherwise(when($"max_coverage_period".isNotNull and $"max_coverage_period".rlike("15D"),$"amt_per_day"*15)
        .otherwise(when($"max_coverage_period".isNull and $"benefit_allowed_days".isNotNull,$"amt_per_day"*$"benefit_allowed_days"))))
        .select("bnfttype","plan_id","Flag","hcb_amt")
        .withColumn("max_hcb",max($"hcb_amt").over(win1)).where($"hcb_amt"===$"max_hcb").distinct
     .groupBy("plan_id").agg(sum("max_hcb").as("tot_hcb"))


    spark.sql("")
  }

  /*CASE
            WHEN b.max_coverage_period is NOT null
                AND (b.max_coverage_period LIKE '%30D%'
                OR b.max_coverage_period LIKE '%1M%') THEN
            cast(b.amt_per_day AS double)*30
            WHEN b.max_coverage_period is NOT null
                AND b.max_coverage_period LIKE '%15D%' THEN
            cast(b.amt_per_day AS double)*15
            WHEN b.max_coverage_period is null
                AND b.benefit_allowed_days is NOT NULL THEN
            cast(b.amt_per_day AS double)*cast(b.benefit_allowed_days AS double)
            END AS hcb_amt
*/

  /*SELECT *
                FROM phoenix_uat_db.bt_plan_limit) b
                    ON trim(a.plan_id) = trim(b.plan_id)
                        AND trim(a.item_no) = trim(b.item_no)
                WHERE BNFTTYPE IS NOT NULL

  plan_id,item_no,benefit,

                CASE
                WHEN trim(benefit) LIKE '%C%' THEN
                'HCB'
                END AS BNFTTYPE,
                CASE
                WHEN trim(benefit) IN ('00C01','00C01') THEN
                'ICU'
                WHEN trim(benefit) IN ('00C') THEN
                'NON ICU'
                END AS Flag*/

}
