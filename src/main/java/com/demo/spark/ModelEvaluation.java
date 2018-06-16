package com.demo.spark;

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.DataFrame;

/**
 * Created by sauraraj on 8/27/2016.
 */
public class ModelEvaluation {

    private double modelAccuracy;
    public ModelEvaluation(DataFrame data){
        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("Actual")
                .setPredictionCol("prediction");
        double accuracy = evaluator.evaluate(data);
        modelAccuracy = accuracy;
    }
    public double getAccuracy(){
        return modelAccuracy;
    }
}
