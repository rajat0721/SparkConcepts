
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object PolicyTbl {

  def getDataFrameWithLatestRecords(spark: SparkSession,
                                    table: String,
                                    dataFrame: DataFrame,
                                    schemaName: String): DataFrame = {
    val tableName = table
    val primaryKeyColumns = spark.read
      .option("multiLine", value = true)
      .json("s3://max-lumiq-edl-uat/misc/primarycolumnlist.json")

    primaryKeyColumns.createOrReplaceTempView("JsonView")
    val columnDf = spark.sql(
      "SELECT primaryColumn from JsonView where tableName = '" + tableName + "' AND schemaName = '" + schemaName + "'"
    )
    if (columnDf.isEmpty)
      dataFrame
    else {
      val primaryColumn = columnDf.collectAsList().get(0).getString(0)
      dataFrame.createOrReplaceTempView(tableName)
      val latestRecordsDF = spark.sql(
        "Select * from " + tableName + " where (" + primaryColumn + ", upd_date) in (Select " + primaryColumn + ",max(upd_date) from " + tableName + " group by " + primaryColumn + ")"
      )
      latestRecordsDF
    }
  }

  def main(args: Array[String]): Unit = {

    if (args.length < 1) {
      println("Job Name Not Provided.")
    }

    val spark = SparkSession.builder().appName("MaxBupaTransformedTables").getOrCreate()

    import spark.implicits._

    //MR_CUSTOMER_TBL
    val mr_customer_tbl_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/MR_CUSTOMER_TBL.parquet")
      .select("CONT_NO", "IS_MASTER_POLICY_CONCEPT", "UPD_DATE")
      .filter("CONT_NO LIKE '3%'")

    val mr_customer_tbl_latest =
      getDataFrameWithLatestRecords(spark, "mr_customer_tbl", mr_customer_tbl_source, "CSR")

    val mr_customer_tbl = mr_customer_tbl_latest.select("CONT_NO", "IS_MASTER_POLICY_CONCEPT")

    mr_customer_tbl.createOrReplaceTempView("mr_customer_tbl")

    //MR_CONTRACT
    val mr_contract_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/MR_CONTRACT.parquet")
      .select("CONT_NO",
        "CONT_YYMM",
        "RENEW_YEAR_NO",
        "ISMHP",
        "ISGCC",
        "ISGCI",
        "ISGHS",
        "EFF_DATE",
        "END_DATE",
        "TERM_DATE",
        "BILL_CYCLE",
        "UPD_DATE")
      .filter("CONT_NO LIKE '3%'")
    val mr_contract_latest =
      getDataFrameWithLatestRecords(spark, "mr_contract", mr_contract_source, "CSR")
    val mr_contract_1 = mr_contract_latest
      .withColumn("POLICYNO", concat($"CONT_NO", $"CONT_YYMM".substr(1, 4), $"RENEW_YEAR_NO"))
      .withColumnRenamed("EFF_DATE", "EFFECTIVEDATE")
      .withColumnRenamed("END_DATE", "ENDDATE")
      .withColumnRenamed("TERM_DATE", "TERMDATE")
      .withColumnRenamed("BILL_CYCLE", "BILLCYCLE")
      .withColumn("PROD_SEGMENT", lit("B2C"))

    //      .withColumnRenamed("PROD_TYPE", "PRODUCT") no need since taken from bt_plan(PROD_TYPE)

    mr_contract_1.createOrReplaceTempView("MR_CONTRACT_1")

    val mr_contract_2 = mr_contract_1.select("POLICYNO",
      "EFFECTIVEDATE",
      "ENDDATE",
      "TERMDATE",
      "BILLCYCLE",
      "CONT_NO",
      "CONT_YYMM",
      "PROD_SEGMENT")

    val mr_contract = mr_contract_2.withColumn("BILLCYCLE",
      when(col("BILLCYCLE")
        .equalTo("Y"),
        "1")
        .otherwise(col("BILLCYCLE")))

    mr_contract.createOrReplaceTempView("mr_contract")

    //BT_PLAN
    val bt_plan_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BT_PLAN.parquet")
      .select("PROD_TYPE",
        "CARD_TYPE",
        "PLAN_ID",
        "PLAN_DESC",
        "PLAN_LIMIT",
        "FLOATER_LIMIT",
        "DEDUCT_AMOUNT",
        "PLAN_TYPE",
        "ADULT_COVERED",
        "CHILD_COVERED",
        "UPD_DATE")

    val bt_plan_latest = getDataFrameWithLatestRecords(spark, "bt_plan", bt_plan_source, "CSR")

    val bt_plan_1 = bt_plan_latest
      .withColumnRenamed("PROD_TYPE", "PRODUCT")
      .withColumnRenamed("CARD_TYPE", "PRODUCTPLAN")
      .withColumnRenamed("PLAN_ID", "PLANID")
      .withColumnRenamed("PLAN_DESC", "PLANDESCRIPTION")
      .withColumnRenamed("PLAN_LIMIT", "PLANLMT")
      .withColumnRenamed("FLOATER_LIMIT", "FLOATERLMT")
      .withColumnRenamed("DEDUCT_AMOUNT", "DEDUCTABLELMT")
      .withColumn("INDIVIDUALMT", $"PLANLMT")
      .withColumn(
        "FMLYTYPE",
        when($"PRODUCT".isin("IFMF") or $"PLAN_TYPE".isin("IFMF"), "Family First").otherwise(
          when($"ADULT_COVERED".cast(IntegerType) + $"CHILD_COVERED".cast(IntegerType) > 1,
            "Family Floater").otherwise("Individual")
        )
      )
      .withColumn("FMLYTYPE1",
        when($"ADULT_COVERED".cast(IntegerType) + $"CHILD_COVERED"
          .cast(IntegerType) > 1,
          "Family Floater").otherwise("Individual"))

    val bt_plan = bt_plan_1.select(
      "FMLYTYPE",
      "FMLYTYPE1",
      "PRODUCT",
      "PRODUCTPLAN",
      "PLANID",
      "PLANDESCRIPTION",
      "FLOATERLMT",
      "INDIVIDUALMT",
      "DEDUCTABLELMT",
      "PLANLMT",
      "ADULT_COVERED",
      "CHILD_COVERED"
    )

    bt_plan.createOrReplaceTempView("bt_plan")

    // MR_MEMBER
    val mr_member_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/MR_MEMBER.parquet")
      .select("MBR_NO",
        "MBR_TYPE",
        "DOB",
        "MBRSHP_NO",
        "EFF_DATE",
        "LOAN_END_DATE",
        "TERM_DATE",
        "CONT_NO",
        "REG_DATE",
        "UPD_DATE")
      .filter("CONT_NO LIKE '3%'")

    val mr_member_latest =
      getDataFrameWithLatestRecords(spark, "mr_member", mr_member_source, "CSR")

    val mr_member_1 = mr_member_latest
      .withColumn("FMLYID", $"MBR_NO".substr(1, 5))
      .withColumnRenamed("DOB", "DOBELDSTMMBR")
      .withColumnRenamed("MBRSHP_NO", "MEMBERSHIPNO")
      .withColumnRenamed("EFF_DATE", "MMBRRCD")
      .withColumnRenamed("LOAN_END_DATE", "MMBRRED")
      .withColumnRenamed("TERM_DATE", "MMBRRTD")

    val mr_member_2 = mr_member_1.select("FMLYID",
      "CONT_NO",
      "DOBELDSTMMBR",
      "MEMBERSHIPNO",
      "MMBRRCD",
      "MMBRRTD",
      "REG_DATE",
      "MBR_NO",
      "MBR_TYPE")

    mr_member_2.createOrReplaceTempView("mr_member")

    val mr_member = spark.sql("""select *,CASE
      WHEN m_m.MBR_TYPE IN ('F','M','H','I','P') THEN '4P'
      WHEN m_m.MBR_TYPE IN ('C','D') THEN '3C'
      WHEN m_m.MBR_TYPE='E' THEN '1E'
      WHEN m_m.MBR_TYPE='S' THEN '2S'
      END AS fmlygrp_cd from mr_member m_m""")

    //BT_PLAN_BENEFIT
    val bt_plan_benefit_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BT_PLAN_BENEFIT.parquet")
      .select("BENEFIT", "PLAN_ID", "ITEM_NO", "NET_CAT", "UPD_DATE")

    val bt_plan_benefit_latest =
      getDataFrameWithLatestRecords(spark, "bt_plan_benefit", bt_plan_benefit_source, "CSR")

    val bt_plan_benefit_1 = bt_plan_benefit_latest
      .withColumnRenamed("BENEFIT", "BNFTTYPE")
      .withColumnRenamed("PLAN_ID", "PLANID")

    val bt_plan_benefit = bt_plan_benefit_1.select("PLANID", "BNFTTYPE")

    bt_plan_benefit.createOrReplaceTempView("bt_plan_benefit")

    //MR_CONT_CLS
    val mr_cont_cls_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/MR_CONT_CLS.parquet")
      .select("CONT_NO", "CONT_YYMM", "CLS_ID", "PLAN_ID", "UPD_DATE")
      .filter("CONT_NO LIKE '3%'")

    val mr_cont_cls_latest =
      getDataFrameWithLatestRecords(spark, "mr_cont_cls", mr_cont_cls_source, "CSR")

    val mr_cont_cls_1 = mr_cont_cls_latest.withColumnRenamed("PLAN_ID", "PLANID")

    val mr_cont_cls = mr_cont_cls_1.select("CONT_NO", "PLANID", "CONT_YYMM", "UPD_DATE")

    mr_cont_cls.createOrReplaceTempView("mr_cont_cls")

    // Processing Starts here

    val eldestdob = spark
      .sql(
        """select FMLYID,CONT_NO,min(DOBELDSTMMBR) as DOBELDSTMMBR from mr_member group by FMLYID,CONT_NO"""
      )
      .select("FMLYID", "DOBELDSTMMBR", "CONT_NO")

    eldestdob.createOrReplaceTempView("eldestdob")

    ///READ DATA FROM S3

    //val BNFTLMPRMTBL = spark.read.parquet("s3://max-lumiq-edl-uat/transformed/MAXIMUS_GROUP/maximus_bnftlmtprmtbl_group.parquet")

    val BNFTLMPRMTBL = spark.read.parquet(
      "s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/maximus_bnftlmtprmtbl_retail.parquet"
    )

    val polciy_gwp = BNFTLMPRMTBL.select("GWP", "POLICYNO", "MEMBERSHIPNO")

    val totalgwp = polciy_gwp.groupBy("POLICYNO").agg(sum("GWP").alias("TOTALGWP")).distinct

    totalgwp.createOrReplaceTempView("totalgwp")

    val mr_mem_df = mr_member.select("FMLYID", "CONT_NO", "fmlygrp_cd").distinct

    mr_mem_df.createOrReplaceTempView("mr_mem_vw")

    val mr_cls_df = mr_cont_cls.select("PLANID", "CONT_NO", "CONT_YYMM", "UPD_DATE").distinct

    mr_cls_df.createOrReplaceTempView("mr_cls_vw")

    val sy_sys_code_source =
      spark.read.parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/SY_SYS_CODE.parquet")

    val sy_sys_code_inter1 =
      getDataFrameWithLatestRecords(spark, "sy_sys_code", sy_sys_code_source, "CSR")

    val sy_sys_code_inter2 = sy_sys_code_inter1
      .withColumn("sys_type", trim(sy_sys_code_inter1("SYS_TYPE")))
      .withColumn("sys_code", trim(sy_sys_code_inter1("SYS_CODE")))

    // val plan_sys_descp = sy_sys_code_inter2.filter("sys_type= 'PRODUCT_TYPE' or sys_type= 'PLAN_TYPE'").select("sys_type","sys_code","EDESC","UPD_DATE")

    val product_plan_descp = sy_sys_code_inter2
      .filter("sys_type= 'CARD_TYPE'")
      .select("sys_type", "sys_code", "EDESC", "UPD_DATE")

    // val bt_plan_benefit_2 = sy_sys_code_inter2.join(broadcast(bt_plan_benefit_1) as "b_p_b",$"b_p_b.BNFTTYPE" === $"sys_code").select("b_p_b.PLANID","b_p_b.BNFTTYPE")

    val bt_plan_fmny = spark.sql("""select m_m.fmlygrp_cd,b_p.PLANID from mr_mem_vw m_m 
    inner join mr_cls_vw m_c_c on m_c_c.CONT_NO = m_m.CONT_NO
    inner join bt_plan b_p on b_p.PLANID = m_c_c.PLANID""").distinct

    val window = Window.partitionBy(col("PLANID")).orderBy("fmlygrp_cd")
    val sortedDf = bt_plan_fmny.withColumn(
      "FMLY_GROUP_CODE",
      collect_list(substring(col("fmlygrp_cd"), 2, 2)).over(window)
    )
    val sorteddf_window = Window.partitionBy(col("PLANID")).orderBy(desc("FMLY_GROUP_CODE"))
    val bt_plan_family_code_inter = sortedDf
      .withColumn("rownumber", row_number() over sorteddf_window)
      .filter("rownumber=1")
      .drop("rownumber")
      .drop("fmlygrp_cd")
    val bt_plan_family_code =
      bt_plan_family_code_inter.withColumn("FMLY_GROUP_CODE", concat_ws("", $"FMLY_GROUP_CODE"))

    bt_plan_family_code.createOrReplaceTempView("bt_plan_family_code")

    //Taking product of mr_contract
    val POLICYTBL_maximusp_2 =
      spark.sql("""select m_t.POLICYNO,m_t.EFFECTIVEDATE,m_t.ENDDATE,m_t.TERMDATE,b_p.PRODUCT,b_p.PRODUCTPLAN,m_c.UPD_DATE,
CASE
  WHEN  (m_t.PROD_SEGMENT='B2C' and b_p.FMLYTYPE='Family Floater' and b_p.CHILD_COVERED = '0') THEN CONCAT(b_p.ADULT_COVERED,'A')
  WHEN  (m_t.PROD_SEGMENT='B2C' and b_p.FMLYTYPE='Family Floater') THEN CONCAT(b_p.ADULT_COVERED,'A',b_p.CHILD_COVERED,'C')
  WHEN  (m_t.PROD_SEGMENT='B2C' and b_p.FMLYTYPE='Individual') THEN '1A'
  WHEN  (m_t.PROD_SEGMENT='B2C' and b_p.FMLYTYPE='Family First') THEN 'Others'
  WHEN m_t.PROD_SEGMENT='B2B' or m_t.PROD_SEGMENT = 'B20' THEN b_p_f_c.FMLY_GROUP_CODE
END AS FMLYGROUP,tg.TOTALGWP,m_m.FMLYID,b_p.PLANID,m_t.PROD_SEGMENT as LOB,m_t.BILLCYCLE,b_p.PLANDESCRIPTION,b_p.FMLYTYPE,b_p.FMLYTYPE1,ed.DOBELDSTMMBR from mr_contract m_t 
inner join mr_mem_vw m_m on m_m.CONT_NO = m_t.CONT_NO 
inner join mr_cls_vw m_c on m_t.CONT_NO = m_c.CONT_NO and m_t.CONT_YYMM = m_c.CONT_YYMM 
inner join bt_plan b_p on b_p.PLANID = m_c.PLANID 
inner join eldestdob ed on ed.FMLYID = m_m.FMLYID and ed.CONT_NO = m_m.CONT_NO
inner join totalgwp tg on tg.POLICYNO = m_t.POLICYNO
inner join bt_plan_family_code b_p_f_c on b_p_f_c.PLANID = b_p.PLANID""").distinct

    POLICYTBL_maximusp_2.createOrReplaceTempView("POLICYTBL_maximusp_2")

    val POLICYTBL_maximusp_1 = spark
      .sql(
        """SELECT T.* from (SELECT A.*,ROW_NUMBER() OVER (partition BY POLICYNO,FMLYID ORDER BY UPD_DATE desc) rnk from  POLICYTBL_maximusp_2 A) T where rnk=1"""
      )
      .drop("rnk", "UPD_DATE")

    POLICYTBL_maximusp_1
      .repartition(10)
      .write
      .mode("overwrite")
      .parquet("s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/policy_retail_inter.parquet")

    val policy_inter = spark.read.parquet(
      "s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/policy_retail_inter.parquet"
    )

    policy_inter.createOrReplaceTempView("policy_inter")

    val POLICY_MAXIMUSP_2 = spark.sql("""select policy_inter.*,CASE 
                                                    WHEN trim(PRODUCT) in ('IFFT','IIND','IFMF') THEN 'HeartBeat' 
                                                    WHEN trim(PRODUCT) LIKE 'IH%' THEN 'Health Companion' 
                                                    WHEN trim(PRODUCT) = 'IIHR' THEN 'Health Recharge' 
                                                    WHEN trim(PRODUCT) = 'IIHA' THEN 'Health Assurance' 
                                                    WHEN trim(PRODUCT) = 'IIHP' THEN 'Health Plus' 
                                                    WHEN trim(PRODUCT) = 'IPMH' THEN 'Micro Health' 
                                                    WHEN trim(PRODUCT) = 'IFSP' THEN 'Swasth Parivar' 
                                                    WHEN trim(PRODUCT) = 'IRSB' THEN 'RSBY' 
                                                    WHEN trim(PRODUCT) = 'IMEP' THEN 'Medical Emergency'
                                                    WHEN trim(PRODUCT) = 'IIGA' THEN 'GoActive'
                                                    END AS PRODUCT1 
                                                    from policy_inter
                                                    """)

    val policy_maximusp_1 =
      POLICY_MAXIMUSP_2.drop("PRODUCT").withColumnRenamed("PRODUCT1", "PRODUCT")

    val policy_maximusp_2 = policy_maximusp_1
      .join(product_plan_descp as "p_p_d", trim($"p_p_d.sys_code") === trim($"PRODUCTPLAN"), "left")
      .drop("UPD_DATE", "sys_type", "sys_code", "PRODUCTPLAN")
      .withColumnRenamed("EDESC", "PRODUCTPLAN")
      .distinct

    val policy_maximusp = policy_maximusp_2
      .withColumn("EFFECTIVEDATE",
        date_format(to_date(to_timestamp($"EFFECTIVEDATE", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn("ENDDATE",
        date_format(to_date(to_timestamp($"ENDDATE", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn("TERMDATE",
        date_format(to_date(to_timestamp($"TERMDATE", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn("DOBELDSTMMBR",
        date_format(to_date(to_timestamp($"DOBELDSTMMBR", "yyyy-MM-dd")), "dd-MMM-yy"))
      .distinct

    policy_maximusp
      .select(
        "POLICYNO",
        "EFFECTIVEDATE",
        "ENDDATE",
        "TERMDATE",
        "PRODUCT",
        "PRODUCTPLAN",
        "TOTALGWP",
        "FMLYID",
        "PLANID",
        "LOB",
        "BILLCYCLE",
        "PLANDESCRIPTION",
        "FMLYTYPE",
        "FMLYTYPE1",
        "FMLYGROUP",
        "DOBELDSTMMBR"
      )
      .withColumn("TOTALGWP", 'TOTALGWP.cast("Decimal(32,2)"))
      .repartition(10)
      .write
      .mode("overwrite")
      .parquet(
        "s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/maximus_policy_retail.parquet"
      )
  }
}