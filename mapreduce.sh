. /home/MukeshPullabhatla/PRAC/Unilever_Project/parameter

hdfs dfs -test -d "/${mr_output_path}"
if [ $? -eq 0 ]
	then
	echo     "***********************************************************************"
	echo     " output path already exists deleting and executing the mapreduce job   "  
	echo     "***********************************************************************"
	hdfs dfs -rm -r "/${mr_output_path}"
fi 
	echo     "************************************************************************"
	echo     "              Mapreduce execution started                               "
	echo     "************************************************************************"
jar -xvf "$RunnableJarName"
yarn jar "$RunnableJarName" "$DriverClassName" "/${Hdfsdir}${timeStamp}/$Inputfile" "/${mr_output_path}"


