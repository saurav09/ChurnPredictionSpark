# ChurnPredictionSpark

An Apache Spark application to predict customers churn. 

Apache Spark version used - 1.6 </br>
ML library used - Spark MLlib </br>


A look of the data

State|Account_Length|Area_Code|Phone|Intl_Plan|VMail_Plan|VMail_Message|Day_Mins|Day_Calls|Day_Charge|Eve_Mins|Eve_Calls|Eve_Charge|Night_Mins|Night_Calls|Night_Charge|Intl_Mins|Intl_Calls|Intl_Charge|CustServ_Calls|Churn
----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----|----
KS|128|415|382-4657|no|yes|25|265.100000|110|45.070000|197.400000|99|16.780000|244.700000|91|11.010000|10.000000|3|2.700000|1|False.
OH|107|415|371-7191|no|yes|26|161.600000|123|27.470000|195.500000|103|16.620000|254.400000|103|11.450000|13.700000|3|3.700000|1|False.
NJ|137|415|358-1921|no|no|0|243.400000|114|41.380000|121.200000|110|10.300000|162.600000|104|7.320000|12.200000|5|3.290000|0|False.
OH|84|408|375-9999|yes|no|0|299.400000|71|50.900000|61.900000|88|5.260000|196.900000|89|8.860000|6.600000|7|1.780000|2|False.
OK|75|415|330-6626|yes|no|0|166.700000|113|28.340000|148.300000|122|12.610000|186.900000|121|8.410000|10.100000|3|2.730000|3|False.
AL|118|510|391-8027|yes|no|0|223.400000|98|37.980000|220.600000|101|18.750000|203.900000|118|9.180000|6.300000|6|1.700000|0|False.
