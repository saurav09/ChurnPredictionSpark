package com.demo.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Model;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.IOException;

/**
 * Created by sauraraj on 8/26/2016.
 */
public class CustomerChurnPrediction {

  public static void main(String[] args) throws IOException {

    SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("Customer Churn Prediction");
    JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
    SQLContext sqlContext = new SQLContext(javaSparkContext);
    System.setProperty("hadoop.home.dir", "C:\\hadoop-2.6.4");


    // Dataset Import
    String customersData = "src/main/resources/churn.csv";
    DataFrame customersDF = sqlContext.read().format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load(customersData);

    // Dataset Preprocessing
    DatasetPreprocessing datasetPreprocessing = new DatasetPreprocessing(customersDF);
    DataFrame customersDataProcessed = datasetPreprocessing.getPreprocessedData();
    customersDataProcessed.show(10);

    // Dataset split into training data and test data.
    DataFrame[] splits = customersDataProcessed.randomSplit(new double[]{0.7, 0.3});
    DataFrame trainingData = splits[0];
    DataFrame testData = splits[1];

    // Model Generation
    ModelGeneration modelGeneration = new ModelGeneration(trainingData);
    Pipeline pipeline = modelGeneration.getPipeline();
    PipelineModel model = modelGeneration.getModel();
    model.save("");
    RandomForestClassifier rfc = modelGeneration.getClassifier();

    // Prediction using generated model
    ModelPrediction modelPrediction = new ModelPrediction(model, testData);
    DataFrame predictions = modelPrediction.getPredictions();
    System.out.printf("Prediction result");
    predictions.select("prediction", "Actual").show(5);

    // Performance parameters of the model
    ModelEvaluation modelEvaluation = new ModelEvaluation(predictions);
    Double accuracy = modelEvaluation.getAccuracy();
    System.out.println("Accuracy of the model is = " + accuracy);

    // Improvement of the generated model using grid search cross-validation approach.
    ModelImprovement modelImprovement = new ModelImprovement(rfc, pipeline, trainingData);
    Model bestModel = modelImprovement.getBestModel();
    DataFrame bm = bestModel.transform(testData);
    System.out.printf("Prediction result after model improvement");
    bm.select("prediction", "Actual").show(5);

    // Performance parameters of the improved model.
    ModelEvaluation evaluationBestModel = new ModelEvaluation(bm);
    accuracy = evaluationBestModel.getAccuracy();
    System.out.println("Accuracy of the improved model is = " + accuracy);
  }
}
