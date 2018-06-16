package com.demo.spark;

import org.apache.spark.ml.PipelineModel;
import org.apache.spark.sql.DataFrame;

/**
 * Created by sauraraj on 8/27/2016.
 */
public class ModelPrediction {

    private DataFrame dataFrame;
    public ModelPrediction(PipelineModel pipelineModel, DataFrame testData) {
        DataFrame predictions = pipelineModel.transform(testData);
        dataFrame = predictions;
    }
    public DataFrame getPredictions(){
        return dataFrame;
    }
}
