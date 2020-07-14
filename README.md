# Collaborative-Filtering
Big Data Project with the aim of returning the best 25 recommendations for a new user on the base on the common interests between him and other users

Follow the instructions below in order to run the project correctly:
* Download the dataset from http://files.grouplens.org/datasets/movielens/ml-latest.zip and take only the movies.csv and ratings.csv
* Copy them into the files folder inside Collaborative Filtering

## Terraform Changes
Open the main.tf file inside Collaborative Filtering folder and following instructions:
* The S3 bucket name, because every bucket is unique. Change it in Amazon S3 section (line 246 in main.tf )
and its occurrences.
* The number of executors and partitions, in order to check the performance with different cluster architec-
ture. Change them in the cluster steps with name Run Collaborative-Filtering and Run baseline in Our
goal and run steps section (line 590 and 605 in main.tf ). The executors are the number of slaves while the
number of partitions in which to split the RDDs is equal to n ◦ executors × n ◦ cores (4 both for m4.large
and c5.xlarge).
* The AWS credentials inside the aws provider (Setup Environment section) for linking your account to the
script (line 237 in main.tf )
* The instance name that you want to use. Go to Amazon EMR section inside the aws emr cluster resource
(line 502 and 507 in main.tf ). If you want to change the number of slaves inside the EMR cluster, you
have to change the parameter instance count (line 503 in main.tf - each EC2 instance has a proper vCPU
limit)
* If you want to execute only the Collaborative-Filtering with the baseline or also with the query executed
by Athena, you have to comment some code inside the main.tf file. In particular, if you want to execute
only the Collaborative-Filtering and the baseline, comment from 263 to 477 and the steps inside the cluster
from 615 to 724.

## Pyhton Changes
* Inside the methods.py and baseline.py files, in order to get the CSV files from S3, it’s necessary to change
the path given inside sc.textFile() with your S3 bucket name. Edit those in lines 15 and 37 in methods.py
and in lines 31 and 48 in baseline.py


* Go to CollaborativeFiltering/files and zip with the name zippo all the .py files 

## Run the project
* Launch the command terraform init inside the folder where there is the main.tf file to initialize a working directory containing
configuration files.
* Then, you can launch terraform plan in order to check which objects will be created.
* Afterwards, so as to apply this plan, launch the command terraform apply -auto-approve (this action will
take several minutes).
* Finally, if you want to shoot down your infrastructure, launch the command terraform destroy -auto-
approve (-auto-approve allows to skip interactive approval of the plan). Sometimes you have to manually
destroy the S3 bucket from the AWS console
