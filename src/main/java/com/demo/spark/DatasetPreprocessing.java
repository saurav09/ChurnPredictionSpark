package com.demo.spark;

import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.StringType;

/**
 * Created by sauraraj on 8/27/2016.
 */
public class DatasetPreprocessing {
    private DataFrame df;

    public DatasetPreprocessing(DataFrame data){

        DataFrame trainDF = processData(data);
        StringIndexerModel labelIndexer  = new StringIndexer().setInputCol("Churn").setOutputCol("Actual").setHandleInvalid("skip").fit(trainDF);
        StringIndexerModel Intl_PlanIndexer = new StringIndexer().setInputCol("Intl_Plan").setOutputCol("Intl_PlanIndexed").setHandleInvalid("skip").fit(trainDF);
        StringIndexerModel VMail_PlanIndexer = new StringIndexer().setInputCol("VMail_Plan").setOutputCol("VMail_PlanIndexed").setHandleInvalid("skip").fit(trainDF);

        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[] {labelIndexer,Intl_PlanIndexer,VMail_PlanIndexer});

        PipelineModel model = pipeline.fit(trainDF);
        trainDF = model.transform(trainDF);

        trainDF= new VectorAssembler()
                .setInputCols(new String[]{"Account_Length","Area_Code","Intl_PlanIndexed","VMail_PlanIndexed","VMail_Message","Day_Mins",
                        "Day_Calls","Day_Charge","Eve_Mins","Eve_Calls","Eve_Charge","Night_Mins","Night_Calls","Night_Charge","Intl_Mins","Intl_Calls",
                        "Intl_Charge","CustServ_Calls","totalCallingMins"})
                .setOutputCol("features")
                .transform(trainDF);

        df = trainDF;
    }

    public DataFrame getPreprocessedData(){
        return df;
    }

    public static DataFrame processData(DataFrame data){
        data = data.drop("Phone").drop("State");
        data = data.withColumn("totalCallingMins",data.col("Day_Mins").$plus(data.col("Eve_Mins").$plus(data.col("Night_Mins"))));
        return data;
    }
}
