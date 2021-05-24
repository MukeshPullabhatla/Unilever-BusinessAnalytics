. /home/MukeshPullabhatla/PRAC/Unilever_Project/parameter

#!/bin/sh
start-all.sh
mr-jobhistory-daemon.sh start historyserver
hdfs dfsadmin -safemode leave

echo "*******************************************************************************"
echo "     ******* UPDATING THE PARAMETER FILE with current TIMESTAMP value  ********                              "
echo "*******************************************************************************"

currentTime=$(date +"%Y-%m-%d_%H-%M-%S")
sed -i '/^time/d' /home/MukeshPullabhatla/PRAC/Unilever_Project/parameter
echo "timeStamp=\"_"$currentTime"\"" >> /home/MukeshPullabhatla/PRAC/Unilever_Project/parameter


echo "*******************************************************************************"
echo "     ******* DATA INGESTION PHASE STARTED ********                              "
echo "*******************************************************************************"
echo ""
echo ""
echo "*******************************************************************************"
echo "             Copying Inputfile into Hdfs                              "
echo "*******************************************************************************"

sh CopyToHDFS.sh

if [ $? -eq 0 ]
then
echo "*******************************************************************************"
echo "               Successfully Inputfile copied to Hdfs                     "
echo "********************************************************************************"
else
echo " Inputfile not copied"
fi


echo "*******************************************************************************"
echo "     ******* DATA PROCESSING PHASE STARTED ********                              "
echo "*******************************************************************************"
echo ""
echo ""
echo "********************************************************************************"
echo "           Mapreduce Processing Started                             "
echo "********************************************************************************"

sh mapreduce.sh
if [ $? -eq 0 ]
then
echo "*******************************************************************************"
echo "           Succesfully Mapreduce Processing is completed                 "
echo "*******************************************************************************"
else
echo "Mapreduce processing stopped"
fi


echo "***************************************************************"
echo "                 SPARK CORE Processing started"
echo "***************************************************************"
hadoop fs -rm -r /Unilever_SparkProcessing

spark-shell -i sparkcore.sh
if [ $? -eq 0 ]; then
echo "***************************************************************"
echo "      Succesfully SparkCore Processing is completed"
echo "***************************************************************"
else
    echo "SparkCore processing stopped"
fi

echo "***************************************************************"
echo "                 SPARK SQL Processing started"
echo "***************************************************************"

spark-shell -i sparksqlnew.sh
if [ $? -eq 0 ]; then
echo "***************************************************************"
echo "      Succesfully SparkSql Processing is completed"
echo "***************************************************************"
else
    echo "SparkSql processing stopped"
fi
