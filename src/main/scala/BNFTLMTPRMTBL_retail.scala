
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.joda.time._
import org.joda.time.format._
import org.apache.spark.sql.functions.expr
import utility.SparkUtility

object BNFTLMTPRMTBL_retail {

  def getDataFrameWithLatestRecords(
                                     spark: SparkSession,
                                     table: String,
                                     dataFrame: DataFrame,
                                     schemaName: String
                                   ): DataFrame = {
    val tableName = table
    println("Table Name : "+table)
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

    val spark = SparkUtility.init()
    import spark.implicits._

    def convert(date: String, number: Integer): String = {
      val dtt  = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SS")
      val dt1t = DateTime.parse(date, dtt)
      val dte  = dt1t.plusYears(number)
      dte.toString
    }

    spark.udf.register("convert", convert _)

    //CI_INSURED_POLICY_HOLDER:
    val ci_insured_policy_holder_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/CI_INSURED_POLICY_HOLDER.parquet")
      .filter("CONT_NO LIKE '3%'")

    val ci_insured_policy_holder_latest =
      getDataFrameWithLatestRecords(spark, "ci_insured", ci_insured_policy_holder_source, "CSR")

    val ci_insured_policy_holder = ci_insured_policy_holder_latest
      .select("POLICY_NO", "CI_INSURED", "CONT_NO", "CONT_YYMM", "MBRSHP_NO")

    println(ci_insured_policy_holder.count())

    //GO_ACTIVE_PA_INSURED

    val go_active_pa_insured_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/GO_ACTIVE_PA_INSURED.parquet")
      .filter("CONT_NO LIKE '3%'")

    val go_active_pa_insured_latest =
      getDataFrameWithLatestRecords(spark,
        "go_active_pa_insured",
        go_active_pa_insured_source,
        "CSR")

    val go_active_pa_insured = go_active_pa_insured_latest
      .select("POLICY_NO", "pa_insured", "cont_no", "cont_yymm", "mbrshp_no")

    println(go_active_pa_insured.count())

    //MR_CONTRACT_RETAIL
    val mr_contract_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/MR_CONTRACT.parquet")
      .select(
        "CONT_NO",
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
        "UPD_DATE"
      )
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

    val mr_contract_2 = mr_contract_1.select(
      "POLICYNO",
      "EFFECTIVEDATE",
      "ENDDATE",
      "TERMDATE",
      "BILLCYCLE",
      "CONT_NO",
      "CONT_YYMM",
      "PROD_SEGMENT"
    )

    val mr_contract = mr_contract_2.withColumn(
      "BILLCYCLE",
      when(
        col("BILLCYCLE")
          .equalTo("Y"),
        "1"
      ).otherwise(col("BILLCYCLE"))
    )

    mr_contract.createOrReplaceTempView("mr_contract")

    println(mr_contract.count())

    //BT_PLAN
    val bt_plan_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BT_PLAN.parquet")
      .select(
        "PROD_TYPE",
        "CARD_TYPE",
        "PLAN_ID",
        "PLAN_DESC",
        "PLAN_LIMIT",
        "FLOATER_LIMIT",
        "DEDUCT_AMOUNT",
        "PLAN_TYPE",
        "ADULT_COVERED",
        "CHILD_COVERED",
        "PA_LIMIT",
        "CI_LIMIT",
        "HCB_LIMIT",
        "VER_NUM", "UPD_DATE", "CLS_ID"
      )

    val bt_plan_latest =
      getDataFrameWithLatestRecords(spark, "bt_plan", bt_plan_source, "CSR")

    val bt_plan_1 = bt_plan_latest
      .withColumnRenamed("PROD_TYPE", "PRODUCT")
      .withColumnRenamed("CARD_TYPE", "PRODUCTPLAN")
      .withColumnRenamed("PLAN_ID", "PLANID")
      .withColumnRenamed("PLAN_DESC", "PLANDESCRIPTION")
      .withColumnRenamed("PLAN_LIMIT", "PLANLMT1")
      .withColumnRenamed("FLOATER_LIMIT", "FLOATERLMT")
      .withColumnRenamed("DEDUCT_AMOUNT", "DEDUCTABLELMT")
      .withColumn(
        "FMLYTYPE",
        when($"PRODUCT".isin("IFMF") or $"PLAN_TYPE".isin("IFMF"), "Family First")
          .otherwise(
            when($"ADULT_COVERED".cast(IntegerType) + $"CHILD_COVERED".cast(IntegerType) > 1,
              "Family Floater")
              .otherwise("Individual")
          )
      )
      .withColumn(
        "FMLYTYPE1",
        when(
          $"ADULT_COVERED".cast(IntegerType) + $"CHILD_COVERED"
            .cast(IntegerType) > 1,
          "Family Floater"
        ).otherwise("Individual")
      )

    val bt_plan_2 = bt_plan_1.select(
      "FMLYTYPE",
      "FMLYTYPE1",
      "PRODUCT",
      "PRODUCTPLAN",
      "PLANID",
      "PLANDESCRIPTION",
      "FLOATERLMT",
      "DEDUCTABLELMT",
      "PLANLMT1",
      "PA_LIMIT",
      "CI_LIMIT",
      "HCB_LIMIT",
      "VER_NUM",
      "ADULT_COVERED",
      "CHILD_COVERED",
      "PLAN_TYPE",
      "CLS_ID"
    )

    val bt_plan = bt_plan_2
      .withColumn("FLOATERLMT", when(col("FLOATERLMT").isNull, "0").otherwise(col("FLOATERLMT")))
      .withColumn("PLANLMT1", when(col("PLANLMT1").isNull, "0").otherwise(col("PLANLMT1")))
      .withColumn("DEDUCTABLELMT",
        when(col("DEDUCTABLELMT").isNull, "0").otherwise(col("DEDUCTABLELMT")))
      .withColumn("PA_LIMIT", when(col("PA_LIMIT").isNull, "0").otherwise(col("PA_LIMIT")))
      .withColumn("CI_LIMIT", when(col("CI_LIMIT").isNull, "0").otherwise(col("CI_LIMIT")))
      .withColumn("HCB_LIMIT", when(col("HCB_LIMIT").isNull, "0").otherwise(col("HCB_LIMIT")))

    bt_plan.createOrReplaceTempView("bt_plan")

    println(bt_plan.count())
    // MR_MEMBER_RETAIL
    val mr_member_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/MR_MEMBER.parquet")
      .select(
        "MBR_NO",
        "DOB",
        "MBRSHP_NO",
        "EFF_DATE",
        "LOAN_END_DATE",
        "TERM_DATE",
        "CONT_NO",
        "REG_DATE",
        "CLS_ID",
        "UPD_DATE"
      )
      .filter("CONT_NO LIKE '3%'")

    val mr_member_latest =
      getDataFrameWithLatestRecords(spark, "mr_member", mr_member_source, "CSR")

    val mr_member_1 = mr_member_latest
      .withColumn("FMLYID", $"MBR_NO".substr(1, 5))
      .withColumnRenamed("MBRSHP_NO", "MEMBERSHIPNO")
      .withColumnRenamed("EFF_DATE", "EFF_DATE")
      .withColumnRenamed("LOAN_END_DATE", "MMBRRED")
      .withColumnRenamed("TERM_DATE", "MMBRRTD")

    val mr_member = mr_member_1
      .select("FMLYID",
        "CONT_NO",
        "DOB",
        "MEMBERSHIPNO",
        "EFF_DATE",
        "MMBRRTD",
        "REG_DATE",
        "MBR_NO",
        "CLS_ID")
      .withColumn("CLS_ID", trim($"CLS_ID"))

    mr_member.createOrReplaceTempView("mr_member")
    println(mr_member.count())

    //BT_PLAN_BENEFIT
    val bt_plan_benefit_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BT_PLAN_BENEFIT.parquet")
      .select("BENEFIT", "PLAN_ID", "ITEM_NO", "NET_CAT", "UPD_DATE")

    val bt_plan_benefit_latest =
      getDataFrameWithLatestRecords(spark, "bt_plan_benefit", bt_plan_benefit_source, "CSR")

    //bt_plan_benefit_latest.write.mode("overwrite").parquet("/home/hadoop/benefit_src")

    //val read_from_parquet = spark.read.parquet("/home/hadoop/benefit_src")

    val bt_plan_benefit_1 = bt_plan_benefit_latest
      .withColumnRenamed("BENEFIT", "BNFT")
      .withColumnRenamed("PLAN_ID", "PLANID")

    bt_plan_benefit_1.createOrReplaceTempView("bt_plan_benefit_1")

    val bt_plan_benefit_2 = spark.sql(""" select *,CASE
 WHEN BNFT LIKE 'S%' THEN 'CI'
 WHEN BNFT='8' or  BNFT='9' THEN 'CI'
 WHEN BNFT LIKE 'Q%' THEN 'HCB'
 WHEN BNFT LIKE 'P%' THEN 'PA'
 WHEN BNFT LIKE 'A%' THEN 'IPD'
 END AS BENEFIT_CODE
 from bt_plan_benefit_1
 """)

    val bt_plan_benefit_a = bt_plan_benefit_2
      .filter($"BENEFIT_CODE".isNotNull)
      .withColumnRenamed("BENEFIT_CODE", "BENEFIT")
      .drop("UPD_DATE")
      .distinct

    bt_plan_benefit_a.createOrReplaceTempView("bt_plan_benefit_a")

    val benefit_collect = spark.sql(
      "select planid, concat_ws(',',collect_set(BENEFIT)) as benefits FROM bt_plan_benefit_a group by planid"
    )

    benefit_collect.createOrReplaceTempView("benefit_collect")

    val benefit_flags = spark.sql(
      "select planid, CASE WHEN benefits LIKE '%IPD%' THEN '1' ELSE '0' END AS IPD_FLAG, CASE WHEN benefits LIKE '%PA%' THEN '1' ELSE '0' END AS PA_FLAG,CASE WHEN benefits LIKE '%CI%' THEN '1' ELSE '0' END AS CI_FLAG,CASE WHEN benefits LIKE '%HCB%' THEN '1' ELSE '0' END AS HCB_FLAG FROM benefit_collect"
    )

    benefit_flags.createOrReplaceTempView("benefit_flags")

    val bt_plan_benefit = bt_plan_benefit_a.join(benefit_flags, Seq("PLANID"))

    bt_plan_benefit.createOrReplaceTempView("bt_plan_benefit")

    println(bt_plan_benefit.count())

    //MR_CONT_CLS_RETAIL
    val mr_cont_cls_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/MR_CONT_CLS.parquet")
      .select("CONT_NO", "CONT_YYMM", "CLS_ID", "PLAN_ID", "CARD_TYPE", "DISC_LOAD_FTR", "UPD_DATE")
      .filter("CONT_NO LIKE '3%'")
      .withColumn("CLS_ID", trim($"CLS_ID"))

    val mr_cont_cls_latest =
      getDataFrameWithLatestRecords(spark, "mr_cont_cls", mr_cont_cls_source, "CSR")

    val mr_cont_cls_1 =
      mr_cont_cls_latest.withColumnRenamed("PLAN_ID", "PLANID")

    val mr_cont_cls =
      mr_cont_cls_1.select("CONT_NO",
        "PLANID",
        "CONT_YYMM",
        "CARD_TYPE",
        "CLS_ID",
        "DISC_LOAD_FTR",
        "UPD_DATE")

    mr_cont_cls.createOrReplaceTempView("mr_cont_cls_retail")

    println(mr_cont_cls.count())

    val bf_mbr_written = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BF_MBR_WRITTEN.parquet")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "SEQ_NO",
        "a_rate",
        "p_rate",
        "s_rate",
        "q_rate",
        "al_prem_amt",
        "z_rate",
        "x_rate",
        "y_rate",
        "w_rate",
        "al_city_dl_amt",
        "UPD_DATE"
      )
      .filter("CONT_NO LIKE '3%'")

    val bf_mbr_written_1 =
      getDataFrameWithLatestRecords(spark, "bf_mbr_written", bf_mbr_written, "CSR")

    val bf_mbr_written_2 = bf_mbr_written_1.select(
      "CONT_NO",
      "CONT_YYMM",
      "MBR_NO",
      "al_prem_amt",
      "a_rate",
      "p_rate",
      "s_rate",
      "q_rate",
      "z_rate",
      "x_rate",
      "y_rate",
      "w_rate",
      "al_city_dl_amt"
    )
    val bf_mbr_written_3 = bf_mbr_written_2
      .withColumn("a_rate", when(col("a_rate").isNull, "0").otherwise(col("a_rate")))
      .withColumn("x_rate", when(col("x_rate").isNull, "0").otherwise(col("x_rate")))
      .withColumn("p_rate", when(col("p_rate").isNull, "0").otherwise(col("p_rate")))
      .withColumn("s_rate", when(col("s_rate").isNull, "0").otherwise(col("s_rate")))
      .withColumn("q_rate", when(col("q_rate").isNull, "0").otherwise(col("q_rate")))
      .withColumn("z_rate", when(col("z_rate").isNull, "0").otherwise(col("z_rate")))
      .withColumn("w_rate", when(col("w_rate").isNull, "0").otherwise(col("w_rate")))
      .withColumn("y_rate", when(col("y_rate").isNull, "0").otherwise(col("y_rate")))
      .withColumn("al_prem_amt", when(col("al_prem_amt").isNull, "0").otherwise(col("al_prem_amt")))
      .withColumn("al_city_dl_amt",
        when(col("al_city_dl_amt").isNull, "0").otherwise(col("al_city_dl_amt")))

    val bf_mbr_written_4 = bf_mbr_written_3.filter(
      "CAST(al_prem_amt AS BIGINT)>=0 AND CAST(a_rate AS BIGINT)>=0 AND CAST(p_rate AS BIGINT)>=0 AND" +
        " CAST(s_rate AS BIGINT)>=0 AND CAST(q_rate AS BIGINT)>=0 AND CAST(z_rate AS BIGINT)>=0 AND CAST(x_rate AS BIGINT)>=0 AND " +
        "CAST(y_rate AS BIGINT)>=0 AND CAST(w_rate AS BIGINT)>=0"
    )

    bf_mbr_written_4.createOrReplaceTempView("bf_mbr_written")

    val BF_MBR_WRITTEN_BASE = spark.sql(
      "SELECT CONT_NO,CONT_YYMM, SUM(A_RATE) as A_RATE,SUM(AL_PREM_AMT) as AL_PREM_AMT,SUM(AL_CITY_DL_AMT) as AL_CITY_DL_AMT," +
        "SUM(P_RATE) as P_RATE,SUM(Q_RATE) as Q_RATE,SUM(S_RATE) as S_RATE,SUM(Z_RATE) as Z_RATE,SUM(X_RATE) as X_RATE," +
        "SUM(Y_RATE) as Y_RATE,SUM(W_RATE) as W_RATE FROM BF_MBR_WRITTEN GROUP BY CONT_NO,CONT_YYMM"
    )

    BF_MBR_WRITTEN_BASE.createOrReplaceTempView("BF_MBR_WRITTEN_BASE")

    println(BF_MBR_WRITTEN_BASE.count())

    val bf_mbr_payshd_v_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BF_MBR_PAYSHD_V.parquet")
      .filter("CONT_NO LIKE '3%'")

    val bf_mbr_payshd_v_latest =
      getDataFrameWithLatestRecords(spark, "bf_mbr_payshd_v", bf_mbr_payshd_v_source, "CSR")

    val bf_mbr_payshd_v_1 = bf_mbr_payshd_v_latest.select(
      "CONT_NO",
      "CONT_YYMM",
      "MBR_NO",
      "SEQ_NO",
      "DOC_TYPE",
      "INSTALLMENT_NO",
      "INV_AMT",
      "INV_CI_AMT",
      "INV_P_AMT",
      "INV_IIGA_BASE_PREM",
      "INV_HOSPI_CASH_AMT",
      "INV_ECONSULTANCY_AMT",
      "INV_ENH_NCB_AMT",
      "INV_ENH_REFILL_AMT",
      "INV_IIHR_DIS_AMT",
      "INV_MEM_DL_AMT",
      "INV_RM_AMT",
      "UPD_DATE"
    )

    val bf_mbr_payshd_v = bf_mbr_payshd_v_1
      .withColumn("inv_amt", when(col("inv_amt").isNull, "0").otherwise(col("inv_amt")))
      .withColumn("INV_CI_AMT", when(col("INV_CI_AMT").isNull, "0").otherwise(col("INV_CI_AMT")))
      .withColumn("inv_p_amt", when(col("inv_p_amt").isNull, "0").otherwise(col("inv_p_amt")))
      .withColumn(
        "inv_hospi_cash_amt",
        when(col("inv_hospi_cash_amt").isNull, "0").otherwise(col("inv_hospi_cash_amt"))
      )
      .withColumn(
        "inv_econsultancy_amt",
        when(col("inv_econsultancy_amt").isNull, "0").otherwise(col("inv_econsultancy_amt"))
      )
      .withColumn("inv_enh_ncb_amt",
        when(col("inv_enh_ncb_amt").isNull, "0").otherwise(col("inv_enh_ncb_amt")))
      .withColumn(
        "inv_enh_refill_amt",
        when(col("inv_enh_refill_amt").isNull, "0").otherwise(col("inv_enh_refill_amt"))
      )
      .withColumn("inv_iihr_dis_amt",
        when(col("inv_iihr_dis_amt").isNull, "0").otherwise(col("inv_iihr_dis_amt")))
      .withColumn("inv_mem_dl_amt",
        when(col("inv_mem_dl_amt").isNull, "0").otherwise(col("inv_mem_dl_amt")))
      .withColumn("inv_rm_amt", when(col("inv_rm_amt").isNull, "0").otherwise(col("inv_rm_amt")))
      .withColumn(
        "inv_iiga_base_prem",
        when(col("inv_iiga_base_prem").isNull, "0")
          .otherwise(col("inv_iiga_base_prem"))
      )
      .filter("CAST(INV_AMT AS BIGINT)>=0")

    bf_mbr_payshd_v.createOrReplaceTempView("bf_mbr_payshd_v")

    val BF_MBR_PAYSHD_V_BASE =
      spark.sql(
        """SELECT CONT_NO,CONT_YYMM,
      SUM(INV_AMT) as INV_AMT,
      SUM(INV_P_AMT) as INV_P_AMT,
      SUM(INV_IIGA_BASE_PREM) as INV_IIGA_BASE_PREM,
      SUM(INV_HOSPI_CASH_AMT) as INV_HOSPI_CASH_AMT,
      SUM(INV_ECONSULTANCY_AMT) as INV_ECONSULTANCY_AMT,
      SUM(INV_ENH_NCB_AMT) as INV_ENH_NCB_AMT,
      SUM(INV_ENH_REFILL_AMT) as INV_ENH_REFILL_AMT,
      SUM(INV_IIHR_DIS_AMT) as INV_IIHR_DIS_AMT,
      SUM(INV_MEM_DL_AMT) as INV_MEM_DL_AMT,
      SUM(INV_RM_AMT) as INV_RM_AMT,
      SUM(INV_CI_AMT) as INV_CI_AMT FROM bf_mbr_payshd_v GROUP BY CONT_NO,CONT_YYMM"""
      )

    BF_MBR_PAYSHD_V_BASE.createOrReplaceTempView("BF_MBR_PAYSHD_V_BASE")

    println(BF_MBR_PAYSHD_V_BASE.count())

    //BF_TXN
    val bf_txn_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BF_TXN.parquet")
      .select("DOC_NO", "CONT_NO", "CONT_YYMM", "DOC_TYPE", "UPD_DATE")
      .filter("CONT_NO LIKE '3%'")

    val bf_txn_latest =
      getDataFrameWithLatestRecords(spark, "bf_txn", bf_txn_source, "CSR")

    val bf_txn_inter =
      bf_txn_latest.select("DOC_NO", "CONT_NO", "CONT_YYMM", "DOC_TYPE")

    val bf_txn = bf_txn_inter.filter("trim(DOC_TYPE) = 'AV' or trim(DOC_TYPE)= 'IV'").distinct

    bf_txn.createOrReplaceTempView("bf_txn")

    println(bf_txn.count())
    //BF_SETTLEMENT_RETAIL
    val bf_settlement_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BF_SETTLEMENT.parquet")
      .select("CONT_NO",
        "CONT_YYMM",
        "SETTLED_DOC_NO",
        "SETTLE_DATE",
        "settling_doc_no",
        "UPD_DATE")
      .filter("CONT_NO LIKE '3%'")

    val bf_settlement_latest =
      getDataFrameWithLatestRecords(spark, "bf_settlement", bf_settlement_source, "CSR")

    val bf_settlement_1 = bf_settlement_latest.withColumnRenamed("SETTLED_DOC_NO", "DOC_NO")

    val bf_settlement =
      bf_settlement_1.select("CONT_NO", "CONT_YYMM", "DOC_NO", "SETTLE_DATE").distinct

    bf_settlement.createOrReplaceTempView("bf_settlement_retail")

    println(bf_settlement.count())

    //BF_INV_AV_LINE
    val bf_inv_av_line_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BF_INV_AV_LINE.parquet")
      .select(
        "CONT_NO",
        "AL_PREM_AMT",
        "DOC_NO",
        "CONT_YYMM",
        "MBR_NO",
        "CLS_ID",
        "SEQ_NO",
        "INSTALLMENT_NO",
        "UPD_DATE"
      )
      .filter("CONT_NO LIKE '3%'")

    val bf_inv_av_line_latest =
      getDataFrameWithLatestRecords(spark, "bf_inv_av_line", bf_inv_av_line_source, "CSR")

    val bf_inv_av_line_1 = bf_inv_av_line_latest.withColumnRenamed("AL_PREM_AMT", "GWP_amt")

    val bf_inv_av_line =
      bf_inv_av_line_1.select("CONT_NO", "CONT_YYMM", "MBR_NO", "DOC_NO", "GWP_amt", "CLS_ID")

    bf_inv_av_line.createOrReplaceTempView("bf_inv_av_line")

    println(bf_inv_av_line.count())

    //BF_INV_IV_LINE
    val bf_inv_iv_line_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BF_INV_IV_LINE.parquet")
      .select("MBR_NO", "CONT_NO", "INV_AMT", "DOC_NO", "CONT_YYMM", "CLS_ID", "UPD_DATE", "SEQ_NO")
      .filter("CONT_NO LIKE '3%'")

    val bf_inv_iv_line_latest =
      getDataFrameWithLatestRecords(spark, "bf_inv_iv_line", bf_inv_iv_line_source, "CSR")

    val bf_inv_iv_line_1 = bf_inv_iv_line_latest.withColumnRenamed("INV_AMT", "GWP_amt")

    val bf_inv_iv_line =
      bf_inv_iv_line_1.select("CONT_NO", "CONT_YYMM", "MBR_NO", "DOC_NO", "GWP_amt", "CLS_ID")

    bf_inv_iv_line.createOrReplaceTempView("bf_inv_iv_line")

    println(bf_inv_iv_line.count())

    val bf_iv_av_line = bf_inv_iv_line.unionByName(bf_inv_av_line)

    println(bf_iv_av_line.count())

    // Bt_plan_limit:
    val bt_plan_limit_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BT_PLAN_LIMIT.parquet")
      .select("plan_id", "combine_net_ind", "amt_yr", "item_no", "upd_date")

    val bt_plan_limit =
      getDataFrameWithLatestRecords(spark, "bt_plan_limit", bt_plan_limit_source, "CSR")
        .withColumnRenamed("plan_id", "PLANID")
        .drop("UPD_DATE")

    println(bt_plan_limit.count())

    sys.exit(1)

    // Bf_Bfftxnxls01_Hist
    val Bf_Bfftxnxls01_Hist_source = spark.read
      .parquet("s3://max-lumiq-edl-uat/indexed/maximusp/CSR/BF_BFFTXNXLS01_HIST.parquet")
      .select("DOC_NO", "DOC_TYPE", "UPD_DATE", "DATE_VALUE")

    val Bf_Bfftxnxls01_Hist =
      getDataFrameWithLatestRecords(spark, "Bf_Bfftxnxls01_Hist", Bf_Bfftxnxls01_Hist_source, "CSR")
        .drop("UPD_DATE")

    Bf_Bfftxnxls01_Hist.createOrReplaceTempView("Bf_Bfftxnxls01_Hist")

    //Base View:
    val Baseview_interr_1_mid = mr_contract
      .join(mr_cont_cls, Seq("CONT_NO", "CONT_YYMM"), "inner")
      .join(mr_member, Seq("CONT_NO","CLS_ID"), "inner")
      .join(bt_plan, Seq("PLANID"), "inner")
      .join(
        bf_iv_av_line.select("CONT_NO", "CONT_YYMM", "MBR_NO").distinct,
        Seq("CONT_NO", "CONT_YYMM", "MBR_NO"),
        "inner"
      )
      .withColumn("MMBRRCD",
        when($"EFFECTIVEDATE".gt($"REG_DATE"), $"EFFECTIVEDATE").otherwise($"REG_DATE"))
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "POLICYNO",
        "MEMBERSHIPNO",
        "MBR_NO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "ENDDATE",
        "MMBRRCD",
        "REG_DATE",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "FMLYTYPE",
        "FMLYTYPE1",
        "ADULT_COVERED",
        "CHILD_COVERED",
        "PLANDESCRIPTION",
        "PLAN_TYPE",
        "PA_LIMIT",
        "PLANLMT1",
        "CI_LIMIT",
        "HCB_LIMIT",
        "UPD_DATE"
      )
      .distinct

    Baseview_interr_1_mid.createOrReplaceTempView("Baseview_interr_1_mid")

    val Baseview_interr_1_pre = spark
      .sql(
        """SELECT T.* from (SELECT A.*,ROW_NUMBER() OVER (partition BY POLICYNO,MEMBERSHIPNO ORDER BY UPD_DATE desc) rnk from
          | Baseview_interr_1_mid A) T where rnk=1""".stripMargin
      )
      .drop("rnk", "UPD_DATE")

    val Baseview_interr_1_new =
      Baseview_interr_1_pre
        .join(bt_plan_benefit, Seq("PLANID"), "inner")
        .withColumnRenamed("ITEM_NO", "item_no")

    Baseview_interr_1_new
      .repartition(200)
      .write
      .mode("overwrite")
      .parquet("s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/max_dump.parquet")

    val Baseview_interr_1 =
      spark.read.parquet("s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/max_dump.parquet")

    val Baseview_interr_2_new = Baseview_interr_1
    Baseview_interr_2_new.createOrReplaceTempView("Baseview_interr_2_new")

    val totalmembercount_inter =
      Baseview_interr_2_new.select("POLICYNO", "MBR_NO", "FMLYID", "PLANID")

    val totalmembercount =
      totalmembercount_inter
        .groupBy("POLICYNO", "PLANID", "FMLYID")
        .agg(count("MBR_NO").alias("TotalMember"))

    totalmembercount.createOrReplaceTempView("totalmembercount")

    val Baseview_inter_2 = spark.sql(
      "SELECT *,CASE WHEN BENEFIT = 'IPD' AND FMLYTYPE1='Individual' THEN PLANLMT1 ELSE '0' END AS INDIVIDUALLMT,CASE WHEN BENEFIT = 'IPD' AND FMLYTYPE1='Family Floater' THEN PLANLMT1 ELSE FLOATERLMT END AS FLOATERLMT1 FROM Baseview_interr_2_new"
    )

    Baseview_inter_2.createOrReplaceTempView("Baseview_inter_2")

    val Baseview_inter_3_pre = spark
      .sql(
        """SELECT
            CONT_NO,
            IPD_FLAG,PA_FLAG,HCB_FLAG,CI_FLAG,
            CONT_YYMM,
           B_I_2.POLICYNO,
            MEMBERSHIPNO,
            MBR_NO,
            B_I_2.PLANID,
            B_I_2.FMLYID,
            PRODUCT,
            BENEFIT,
            CASE
            WHEN (trim(BENEFIT) = 'PA') THEN PA_LIMIT
            WHEN BENEFIT = 'HCB' AND PRODUCT = 'IIHA' THEN 52*CAST(HCB_LIMIT AS DOUBLE)
            WHEN (BENEFIT = 'HCB' AND (PRODUCT IN ('IIND','IFFT','IFMF','IIHP') OR PRODUCT LIKE 'IH%')) THEN 30*CAST(HCB_LIMIT AS DOUBLE)
            WHEN BENEFIT = 'CI' THEN CI_LIMIT
            WHEN BENEFIT = 'IPD' AND FMLYTYPE = 'Family First'
              THEN tmc.TotalMember*(CAST(INDIVIDUALLMT as DOUBLE)) + CAST(FLOATERLMT1 as DOUBLE)
            ELSE PLANLMT1
            END AS PLANLMT,
            INDIVIDUALLMT,
			FLOATERLMT1,
            DEDUCTABLELMT,
            PROD_SEGMENT,
            BILLCYCLE,
            ENDDATE,
            MMBRRCD,
            REG_DATE,
            MMBRRTD,
            VER_NUM ,
            ADULT_COVERED,CHILD_COVERED,PLANDESCRIPTION,PLAN_TYPE,DOB,EFFECTIVEDATE
            FROM Baseview_inter_2 B_I_2
            inner join totalmembercount tmc on tmc.POLICYNO=B_I_2.POLICYNO and tmc.FMLYID=B_I_2.FMLYID and tmc.PLANID=B_I_2.PLANID"""
      )
      .drop("FLOATERLMT")
      .withColumnRenamed("FLOATERLMT1", "FLOATERLMT")

    Baseview_inter_3_pre.createOrReplaceTempView("Baseview_inter_3")

    //break

    val Baseview_inter =
      spark.sql("""SELECT
            CONT_NO,
            CONT_YYMM,
            IPD_FLAG,PA_FLAG,HCB_FLAG,CI_FLAG,
            POLICYNO,
            MEMBERSHIPNO,
            MBR_NO,
            PROD_SEGMENT,
            PLANID,
            FMLYID,
            PRODUCT,
            BENEFIT,
            PLANLMT,
            INDIVIDUALLMT,
            FLOATERLMT,
            DEDUCTABLELMT,
            MMBRRCD,
            CASE
            WHEN (MMBRRTD IS NOT NULL) THEN convert(EFFECTIVEDATE,CAST(BILLCYCLE as INT))
            ELSE ENDDATE
            END AS MMBRRED,
            MMBRRTD,
            REG_DATE as MMBRFRCD,
            VER_NUM,
            ADULT_COVERED,CHILD_COVERED,PLANDESCRIPTION,BILLCYCLE,PLAN_TYPE,DOB,EFFECTIVEDATE
            FROM Baseview_inter_3""")

    val Baseview = Baseview_inter.withColumn(
      "MMBRRED",
      when($"MMBRRTD".isNull, $"MMBRRED")
        .otherwise($"MMBRRED".cast("timestamp") - expr("INTERVAL 1 DAYS"))
    )

    Baseview.createOrReplaceTempView("Baseview")

    val view_2_inter = Baseview.join(BF_MBR_WRITTEN_BASE, Seq("CONT_NO", "CONT_YYMM"))

    val view_2 = view_2_inter
      .withColumn("IPD", lit("0").cast(DoubleType))
      .withColumn("HCB", lit("0").cast(DoubleType))
      .withColumn("PA", lit("0").cast(DoubleType))
      .withColumn("CI", lit("0").cast(DoubleType))
      .withColumn("Total", lit("0").cast(DoubleType))
      .withColumn("IPD_ratio", lit("0").cast(DoubleType))
      .withColumn("PA_ratio", lit("0").cast(DoubleType))
      .withColumn("CI_ratio", lit("0").cast(DoubleType))
      .withColumn("HCB_ratio", lit("0").cast(DoubleType))

    val IIHP_inter = view_2.filter("PRODUCT='IIHP'")

    val IIHP_BASE = IIHP_inter.join(BF_MBR_PAYSHD_V_BASE, Seq("CONT_NO", "CONT_YYMM"))

    val IIHP_1 = IIHP_BASE
      .withColumn(
        "IPD",
        $"IPD_FLAG" * (IIHP_BASE.col("INV_IIGA_BASE_PREM") + IIHP_BASE
          .col("INV_ECONSULTANCY_AMT") + IIHP_BASE
          .col("INV_ENH_NCB_AMT") + IIHP_BASE.col("INV_ENH_REFILL_AMT"))
      )
      .withColumn("PA", IIHP_BASE.col("INV_P_AMT") * $"PA_FLAG")
      .withColumn("CI", IIHP_BASE.col("INV_CI_AMT") * $"CI_FLAG")
      .withColumn("HCB", IIHP_BASE.col("INV_HOSPI_CASH_AMT") * $"HCB_FLAG")
      .withColumn(
        "Total",
        IIHP_BASE.col("INV_HOSPI_CASH_AMT") * $"HCB_FLAG" + IIHP_BASE
          .col("INV_P_AMT") * $"PA_FLAG" + IIHP_BASE.col(
          "INV_CI_AMT"
        ) * $"CI_FLAG" + $"IPD_FLAG" * (IIHP_BASE.col(
          "INV_IIGA_BASE_PREM"
        ) + IIHP_BASE.col("INV_ECONSULTANCY_AMT") + IIHP_BASE.col("INV_ENH_NCB_AMT") + IIHP_BASE
          .col("INV_ENH_REFILL_AMT"))
      )
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("PA_ratio", $"PA" / $"Total")
      .withColumn("CI_ratio", $"CI" / $"Total")
      .withColumn("HCB_ratio", $"HCB" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )

    val IIHP_FILTER1 = IIHP_1
      .as("v1")
      .join(
        ci_insured_policy_holder.as("v2"),
        $"v1.POLICYNO" === $"v2.POLICY_NO" && $"v1.MEMBERSHIPNO" === $"v2.MBRSHP_NO",
        "left"
      )
      .withColumn("covered", when($"MBRSHP_NO".isNull && $"BENEFIT" === "CI", "0").otherwise("1"))
      //.filter("BENEFIT<>'CI' OR (BENEFIT = 'CI' AND mbrshp_no is not null)")
      .select($"v1.*", $"covered")

    val IIHP = IIHP_FILTER1
      .as("v1")
      .join(
        go_active_pa_insured.as("v2"),
        $"v1.POLICYNO" === $"v2.POLICY_NO" && $"v1.MEMBERSHIPNO" === $"v2.MBRSHP_NO",
        "left"
      )
      .withColumn("covered",
        when($"MBRSHP_NO".isNull && $"BENEFIT" === "PA", "0").otherwise($"covered"))
      //.filter("BENEFIT<>'PA' OR (BENEFIT = 'PA' AND mbrshp_no is not null)")
      .select($"v1.*", $"covered")

    //IIHR : {IPD,PA,CI}
    val IIHR__inter = view_2.filter("PRODUCT='IIHR'")

    val IIHR_BASE = IIHR__inter.join(BF_MBR_PAYSHD_V_BASE, Seq("CONT_NO", "CONT_YYMM"))

    //CHANGE CI _> INV_CI_AMT
    //CHANGE PA -> INV_P_AMT
    val IIHR_1 = IIHR_BASE
      .withColumn("IPD",
        (IIHR_BASE.col("INV_IIGA_BASE_PREM") + IIHR_BASE.col("INV_RM_AMT")) * $"IPD_FLAG")
      .withColumn("PA", IIHR_BASE.col("INV_P_AMT") * $"PA_FLAG")
      .withColumn("CI", IIHR_BASE.col("INV_CI_AMT") * $"CI_FLAG")
      .withColumn(
        "Total",
        IIHR_BASE.col("INV_P_AMT") * $"PA_FLAG" + IIHR_BASE
          .col("INV_CI_AMT") * $"CI_FLAG" + $"IPD_FLAG" * (IIHR_BASE.col(
          "INV_IIGA_BASE_PREM"
        ) + IIHR_BASE.col(
          "INV_RM_AMT"
        ))
      )
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("PA_ratio", $"PA" / $"Total")
      .withColumn("CI_ratio", $"CI" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )

    val IIHR_FILTER1 = IIHR_1
      .as("v1")
      .join(
        ci_insured_policy_holder.as("v2"),
        $"v1.POLICYNO" === $"v2.POLICY_NO" && $"v1.MEMBERSHIPNO" === $"v2.MBRSHP_NO",
        "left"
      )
      .withColumn("covered", when($"MBRSHP_NO".isNull && $"BENEFIT" === "CI", "0").otherwise("1"))
      //.filter("BENEFIT<>'CI' OR (BENEFIT = 'CI' AND mbrshp_no is not null)")
      .select($"v1.*", $"covered")

    val IIHR = IIHR_FILTER1
      .as("v1")
      .join(
        go_active_pa_insured.as("v2"),
        $"v1.POLICYNO" === $"v2.POLICY_NO" && $"v1.MEMBERSHIPNO" === $"v2.MBRSHP_NO",
        "left"
      )
      .withColumn("covered",
        when($"MBRSHP_NO".isNull && $"BENEFIT" === "PA", "0").otherwise($"covered"))
      //.filter("BENEFIT<>'PA' OR (BENEFIT = 'PA' AND mbrshp_no is not null)")
      .select($"v1.*", $"covered")

    //IIGA : {PA,IPD}

    //CHANGING IPD LOGIC :INV_AMT -> INV_AMT - INV_P_AMT
    //IIGA_BASE.col("INV_AMT")-IIGA_BASE.col("INV_P_AMT")
    //for IPD:inv_iiga_base_prem
    val IIGA_inter = view_2.filter("PRODUCT='IIGA'")

    val IIGA_BASE = IIGA_inter.join(BF_MBR_PAYSHD_V_BASE, Seq("CONT_NO", "CONT_YYMM"))

    val IIGA_1 = IIGA_BASE
      .withColumn("IPD", IIGA_BASE.col("inv_iiga_base_prem") * $"IPD_FLAG")
      .withColumn("PA", IIGA_BASE.col("INV_P_AMT") * $"PA_FLAG")
      .withColumn(
        "Total",
        IIGA_BASE.col("INV_P_AMT") * $"PA_FLAG" + IIGA_BASE.col("inv_iiga_base_prem") * $"IPD_FLAG"
      )
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("PA_ratio", $"PA" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )

    val IIGA_FILTER1 = IIGA_1
      .as("v1")
      .join(
        ci_insured_policy_holder.as("v2"),
        $"v1.POLICYNO" === $"v2.POLICY_NO" && $"v1.MEMBERSHIPNO" === $"v2.MBRSHP_NO",
        "left"
      )
      .withColumn("covered", when($"MBRSHP_NO".isNull && $"BENEFIT" === "CI", "0").otherwise("1"))
      //.filter("BENEFIT<>'CI' OR (BENEFIT = 'CI' AND mbrshp_no is not null)")
      .select($"v1.*", $"covered")

    val IIGA = IIGA_FILTER1
      .as("v1")
      .join(
        go_active_pa_insured.as("v2"),
        $"v1.POLICYNO" === $"v2.POLICY_NO" && $"v1.MEMBERSHIPNO" === $"v2.MBRSHP_NO",
        "left"
      )
      .withColumn("covered",
        when($"MBRSHP_NO".isNull && $"BENEFIT" === "PA", "0").otherwise($"covered"))
      //.filter("BENEFIT<>'PA' OR (BENEFIT = 'PA' AND mbrshp_no is not null)")
      .select($"v1.*", $"covered")

    val IFMF_BASE = view_2.filter("PRODUCT='IFMF'")

    val IFMF = IFMF_BASE
      .withColumn(
        "IPD",
        (
          IFMF_BASE.col("a_rate") + IFMF_BASE.col("w_rate") + IFMF_BASE.col("x_rate") + IFMF_BASE
            .col("y_rate") + IFMF_BASE.col("al_city_dl_amt")
          ) * $"IPD_FLAG"
      )
      .withColumn("HCB", IFMF_BASE.col("q_rate") * $"HCB_FLAG")
      .withColumn(
        "Total",
        $"IPD_FLAG" * (IFMF_BASE.col("a_rate") + IFMF_BASE.col("w_rate") + IFMF_BASE
          .col("x_rate") + IFMF_BASE
          .col("y_rate") + IFMF_BASE.col("al_city_dl_amt")) + IFMF_BASE.col("q_rate") * $"HCB_FLAG"
      )
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("HCB_ratio", $"HCB" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )
      .withColumn("covered", lit("1").cast(StringType))

    //IFFT_IIND : {IPD,HCB}
    val IFFT_IIND_BASE = view_2.filter("PRODUCT IN ('IFFT','IIND')")

    val IFFT_IIND = IFFT_IIND_BASE
      .withColumn(
        "IPD",
        (IFFT_IIND_BASE.col("a_rate") + IFFT_IIND_BASE.col("al_city_dl_amt")) * $"IPD_FLAG"
      )
      .withColumn("HCB", IFFT_IIND_BASE.col("q_rate") * $"HCB_FLAG")
      .withColumn(
        "Total",
        (IFFT_IIND_BASE.col("a_rate") + IFFT_IIND_BASE
          .col("al_city_dl_amt")) * $"IPD_FLAG" + IFFT_IIND_BASE.col("q_rate") * $"HCB_FLAG"
      )
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("HCB_ratio", $"HCB" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )
      .withColumn("covered", lit("1").cast(StringType))

    IFFT_IIND.createOrReplaceTempView("IFFT_IIND")

    // #####################Completed

    //IH% {IPD,HCB}
    val IH_BASE = view_2.filter($"PRODUCT".like("IH%"))

    val IH = IH_BASE
      .withColumn("IPD", (IH_BASE.col("a_rate") + IH_BASE.col("al_city_dl_amt")) * $"IPD_FLAG")
      .withColumn("HCB", IH_BASE.col("q_rate") * $"HCB_FLAG")
      .withColumn("Total",
        (IH_BASE.col("a_rate") + IH_BASE.col("al_city_dl_amt")) * $"IPD_FLAG" + IH_BASE
          .col("q_rate") * $"HCB_FLAG")
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("HCB_ratio", $"HCB" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )
      .withColumn("covered", lit("1").cast(StringType))

    IH.createOrReplaceTempView("IH")

    //IRSB,IMEP,IFSP {IPD}
    val IRSB_IMEP_IFSP_BASE = view_2.filter("PRODUCT IN ('IRSB','IMEP','IFSP')")

    val IRSB_IMEP_IFSP = IRSB_IMEP_IFSP_BASE
      .withColumn("IPD", IRSB_IMEP_IFSP_BASE.col("AL_PREM_AMT") * $"IPD_FLAG")
      .withColumn("Total", IRSB_IMEP_IFSP_BASE.col("AL_PREM_AMT") * $"IPD_FLAG")
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )
      .withColumn("covered", lit("1").cast(StringType))

    IRSB_IMEP_IFSP.createOrReplaceTempView("IRSB_IMEP_IFSP")

    //IPMH : {IPD,PA}
    val IPMH_BASE = view_2.filter("PRODUCT = 'IPMH'")

    val IPMH = IPMH_BASE
      .withColumn("IPD", IPMH_BASE.col("AL_PREM_AMT") * $"IPD_FLAG")
      .withColumn("PA", IPMH_BASE.col("p_rate") * $"PA_FLAG")
      .withColumn("Total",
        IPMH_BASE.col("AL_PREM_AMT") * $"IPD_FLAG" + IPMH_BASE.col("p_rate") * $"PA_FLAG")
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("PA_ratio", $"PA" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )
      .withColumn("covered", lit("1").cast(StringType))

    IPMH.createOrReplaceTempView("IPMH")

    //IIHA V1 : {PA,CI,HCB}

    val IIHA_V1_BASE = view_2.filter("PRODUCT='IIHA' AND VER_NUM = '1'")

    val IIHA_V1 = IIHA_V1_BASE
      .withColumn("PA", IIHA_V1_BASE.col("p_rate") * $"PA_FLAG")
      .withColumn("CI", IIHA_V1_BASE.col("s_rate") * $"CI_FLAG")
      .withColumn("HCB", IIHA_V1_BASE.col("q_rate") * $"HCB_FLAG")
      .withColumn(
        "Total",
        IIHA_V1_BASE.col("s_rate") * $"CI_FLAG" + IIHA_V1_BASE
          .col("q_rate") * $"HCB_FLAG" + IIHA_V1_BASE.col("p_rate") * $"PA_FLAG"
      )
      .withColumn("PA_ratio", $"PA" / $"Total")
      .withColumn("CI_ratio", $"CI" / $"Total")
      .withColumn("HCB_ratio", $"HCB" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )
      .withColumn("covered", lit("1").cast(StringType))

    IIHA_V1.createOrReplaceTempView("IIHA_V1")

    //IIHA V2 : {IPD,PA,CI,HCB}

    val IIHA_V2_BASE = view_2.filter("PRODUCT='IIHA' AND VER_NUM = '2'")

    val IIHA_V2 = IIHA_V2_BASE
      .withColumn("HCB", IIHA_V2_BASE.col("Q_RATE") * $"HCB_FLAG")
      .withColumn("PA", (IIHA_V2_BASE.col("P_RATE") + IIHA_V2_BASE.col("Z_RATE")) * $"PA_FLAG")
      .withColumn("CI", IIHA_V2_BASE.col("S_RATE") * $"CI_FLAG")
      .withColumn(
        "Total",
        IIHA_V2_BASE.col("Q_RATE") * $"HCB_FLAG" + $"PA_FLAG" * (IIHA_V2_BASE
          .col("P_RATE") + IIHA_V2_BASE.col("Z_RATE")) + IIHA_V2_BASE.col("S_RATE") * $"CI_FLAG"
      )
      .withColumn("IPD_ratio", $"IPD" / $"Total")
      .withColumn("HCB_ratio", $"HCB" / $"Total")
      .withColumn("PA_ratio", $"PA" / $"Total")
      .withColumn("CI_ratio", $"CI" / $"Total")
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "PLANID",
        "PRODUCT",
        "FMLYID",
        "PROD_SEGMENT",
        "BILLCYCLE",
        "MMBRFRCD",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "DOB",
        "EFFECTIVEDATE",
        "VER_NUM",
        "IPD_ratio",
        "PA_ratio",
        "CI_ratio",
        "HCB_ratio",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "BENEFIT"
      )
      .withColumn("covered", lit("1").cast(StringType))

    val Product_level_view_inter = IIHP
      .unionByName(IIHR)
      .unionByName(IIGA)
      .unionByName(IFFT_IIND)
      .unionByName(IH)
      .unionByName(IRSB_IMEP_IFSP)
      .unionByName(IPMH)
      .unionByName(IIHA_V1)
      .unionByName(IIHA_V2)
      .unionByName(IFMF)

    //Completed


    val Product_level_view = Product_level_view_inter
      .withColumn(
        "IPD_ratio",
        when(col("IPD_ratio").isNull, lit("0").cast(DoubleType)).otherwise(col("IPD_ratio"))
      )
      .withColumn(
        "PA_ratio",
        when(col("PA_ratio").isNull, lit("0").cast(DoubleType)).otherwise(col("PA_ratio"))
      )
      .withColumn(
        "CI_ratio",
        when(col("CI_ratio").isNull, lit("0").cast(DoubleType)).otherwise(col("CI_ratio"))
      )
      .withColumn(
        "HCB_ratio",
        when(col("HCB_ratio").isNull, lit("0").cast(DoubleType)).otherwise(col("HCB_ratio"))
      )
      .distinct

    Product_level_view.createOrReplaceTempView("Product_level_view")

    //VIEW 3 : IV/AV LINE(Union) X Bf_settlement X Bf_txn

    //#################################
    // Completed

    val view_3 = bf_iv_av_line
      .join(bf_settlement, Seq("CONT_NO", "CONT_YYMM", "DOC_NO"))
      .join(bf_txn, Seq("CONT_NO", "CONT_YYMM", "DOC_NO"))
      .select("CONT_NO", "CONT_YYMM", "MBR_NO", "GWP_amt", "SETTLE_DATE", "CLS_ID", "DOC_NO")

    view_3.createOrReplaceTempView("view_3")

    val final_view_inter = spark
      .sql("""
            select  p_v.*,v3.SETTLE_DATE,v3.DOC_NO,
            CASE
              WHEN p_v.BENEFIT = 'IPD' THEN p_v.IPD_ratio*v3.GWP_amt
              WHEN p_v.BENEFIT = 'HCB' THEN p_v.HCB_ratio*v3.GWP_amt
              WHEN p_v.BENEFIT = 'CI' THEN p_v.CI_ratio*v3.GWP_amt
              WHEN p_v.BENEFIT = 'PA' THEN p_v.PA_ratio*v3.GWP_amt
            END as GWP
            from Product_level_view p_v
            inner join view_3 v3 on v3.CONT_NO=p_v.CONT_NO AND v3.CONT_YYMM=p_v.CONT_YYMM AND v3.MBR_NO=p_v.MBR_NO
            """)
      .withColumnRenamed("BENEFIT", "BNFTTYPE")
      .withColumnRenamed("SETTLE_DATE", "SETTLEMENTDATE")
      .withColumn("PRMEFFECTIVEDATE",when($"MMBRRCD".gt($"SETTLEMENTDATE"), $"MMBRRCD")
        .otherwise($"SETTLEMENTDATE"))

    /*val bf_bfftxnls01_hist_1 =
      spark.sql("""SELECT TRIM(doc_no) AS doc_no,max(TRIM(Date_Value))  AS Batchrun_date
FROM Bf_Bfftxnxls01_Hist WHERE trim(doc_type) in('AV','IV') group by TRIM(doc_no)""")

    val final_view = final_view_inter.join(bf_bfftxnls01_hist_1, Seq("DOC_NO"), "left")

    val BNFTLMTPRMTBL_inter = final_view
      .select(
        "CONT_NO",
        "CONT_YYMM",
        "MBR_NO",
        "POLICYNO",
        "MEMBERSHIPNO",
        "BNFTTYPE",
        "PLANLMT",
        "INDIVIDUALLMT",
        "FLOATERLMT",
        "DEDUCTABLELMT",
        "GWP",
        "PRMEFFECTIVEDATE",
        "SETTLEMENTDATE",
        "FMLYID",
        "MMBRRCD",
        "MMBRRED",
        "MMBRRTD",
        "MMBRFRCD",
        "doc_no",
        "covered"
      )*/

    //BNFTLMTPRMTBL_inter
      .filter("covered = '1' OR (covered = '0' AND gwp <> 0)")
      .repartition(10)
      .write
      .mode("overwrite")
      .parquet("s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/earning_inter_fixed.parquet")

    val BNFTLMTPRMTBL_nonformatted_1 = spark.read.parquet(
      "s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/earning_inter_fixed.parquet"
    )

    BNFTLMTPRMTBL_nonformatted_1.createOrReplaceTempView("BNFTLMTPRMTBL_nonformatted_1")

    val BNFTLMTPRMTBL_nonformatted_2 = spark.sql(
      "SELECT a.* , b.post_year , b.post_month FROM (BNFTLMTPRMTBL_nonformatted_1 a LEFT JOIN " +
        "( SELECT * FROM ( SELECT * , row_number() OVER (PARTITION BY DOC_NO ORDER BY POST_YEAR ASC) rownum FROM " +
        "( SELECT * FROM max_bupa_edl.09_10_non_issue_cases UNION ALL " +
        "SELECT * FROM max_bupa_edl.14_15_non_issue_cases UNION ALL SELECT * FROM max_bupa_edl.15_16_non_issue_cases " +
        "UNION ALL SELECT * FROM max_bupa_edl.16_17_non_issue_cases UNION ALL SELECT * FROM max_bupa_edl.17_18_non_issue_cases " +
        "UNION ALL SELECT * FROM max_bupa_edl.18_19_non_issue_cases ) ) WHERE rownum = 1 ) b ON a.doc_no = b.doc_no)"
    )
    val BNFTLMTPRMTBL = BNFTLMTPRMTBL_nonformatted_2
      .withColumn("MMBRRCD",
        date_format(to_date(to_timestamp($"MMBRRCD", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn("MMBRRED",
        date_format(to_date(to_timestamp($"MMBRRED", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn("MMBRRTD",
        date_format(to_date(to_timestamp($"MMBRRTD", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn("MMBRFRCD",
        date_format(to_date(to_timestamp($"MMBRFRCD", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn(
        "PRMEFFECTIVEDATE",
        date_format(to_date(to_timestamp($"PRMEFFECTIVEDATE", "yyyy-MM-dd")), "dd-MMM-yy")
      )
      .withColumn("SETTLEMENTDATE",
        date_format(to_date(to_timestamp($"SETTLEMENTDATE", "yyyy-MM-dd")), "dd-MMM-yy"))
      .withColumn("PLANLMT", 'PLANLMT.cast("Decimal(32,0)"))
      .withColumn("INDIVIDUALLMT", 'INDIVIDUALLMT.cast("Decimal(32,0)"))
      .withColumn("FLOATERLMT", 'FLOATERLMT.cast("Decimal(32,0)"))
      .withColumn("DEDUCTABLELMT", 'DEDUCTABLELMT.cast("Decimal(32,0)"))
      .withColumn("GWP", 'GWP.cast("Decimal(32,2)"))

    BNFTLMTPRMTBL
      .repartition(10)
      .write
      .mode("overwrite")
      .parquet(
        "s3://max-lumiq-edl-uat/transformed/MAXIMUS_RETAIL_NEW/maximus_bnftlmtprmtbl_retail_fixed.parquet"
      )
  }
}