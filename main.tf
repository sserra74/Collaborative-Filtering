#RICORDA CHE QUANDO DEVI ACCEDERE AL MASTER DEVI USARE USERNAME HADOOP


resource "aws_default_vpc" "default" {

  enable_dns_hostnames = true
  enable_dns_support = true

  tags = {
    Name = "Default VPC"
  }
}

resource "aws_default_subnet" "default" {
  availability_zone = "us-east-1b"

  tags = {
    Name = "Default subnet"
  }
}



resource "aws_iam_role" "spark_cluster_iam_emr_service_role" {
    name = "spark_cluster_emr_service_role"
 
    assume_role_policy = <<EOF
{
    "Version": "2008-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "elasticmapreduce.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
          "Sid": "Stmt1547414166585",
          "Effect": "Allow",
          "Principal":{
                "Service": "athena.amazonaws.com"
            },  
          "Action": "sts:AssumeRole"
        }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "emr-service-policy-attach" {
   role = aws_iam_role.spark_cluster_iam_emr_service_role.id
   for_each = toset([
    "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceRole",
    "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
   ])
   policy_arn= each.value
}



resource "aws_iam_role" "spark_cluster_iam_emr_profile_role" {
    name = "spark_cluster_emr_profile_role"
    assume_role_policy = <<EOF
{
    "Version": "2008-10-17",
    "Statement": [
        {
            "Sid": "",
            "Effect": "Allow",
            "Principal": {
                "Service": "ec2.amazonaws.com"
            },
            "Action": "sts:AssumeRole"
        },
        {
          "Sid": "Stmt1547414166585",
          "Effect": "Allow",
          "Principal":{
                "Service": "athena.amazonaws.com"
            },
          "Action": "sts:AssumeRole"
        }
    ]
}
EOF
}

resource "aws_iam_role_policy_attachment" "emr-profile-policy-attach" {
   role = aws_iam_role.spark_cluster_iam_emr_profile_role.id
   #policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role"
   for_each = toset([
    "arn:aws:iam::aws:policy/service-role/AmazonElasticMapReduceforEC2Role",
    "arn:aws:iam::aws:policy/AmazonAthenaFullAccess"
   ])
   policy_arn= each.value
}

resource "aws_iam_instance_profile" "emr_profile" {
   name = "spark_cluster_emr_profile"
   role = aws_iam_role.spark_cluster_iam_emr_profile_role.name
}

data "http" "myip" {
  url = "http://ipv4.icanhazip.com"
}


resource "aws_security_group" "master_security_group" {
  name        = "master_security_group"
  description = "Allow inbound traffic from VPN"
  vpc_id      = aws_default_vpc.default.id
 
  # Avoid circular dependencies stopping the destruction of the cluster
  revoke_rules_on_delete = true
 
  # Allow communication between nodes in the VPC
  ingress {
    from_port   = "0"
    to_port     = "0"
    protocol    = "-1"
    self        = true
  }
 
  ingress {
      from_port   = "8443"
      to_port     = "8443"
      protocol    = "TCP"
  }
 
  egress {
    from_port   = "0"
    to_port     = "0"
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
 
  # Allow SSH traffic from VPN
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "TCP"
    cidr_blocks = ["${chomp(data.http.myip.body)}/32"]
  }
 
  #### Expose web interfaces to VPN
 
  # Yarn
  ingress {
    from_port   = 8088
    to_port     = 8088
    protocol    = "TCP"
    cidr_blocks = ["${chomp(data.http.myip.body)}/32"]
  }
 
  # Spark History
  ingress {
      from_port   = 18080
      to_port     = 18080
      protocol    = "TCP"
      cidr_blocks = ["${chomp(data.http.myip.body)}/32"]
    }
 

 
  # Spark UI
  ingress {
      from_port   = 4040
      to_port     = 4040
      protocol    = "TCP"
      cidr_blocks = ["${chomp(data.http.myip.body)}/32"]
  }
 
 
 
  lifecycle {
    ignore_changes = [ingress, egress]
  }
 
  tags = {
    name = "emr_test"
  }
}



resource "aws_security_group" "slave_security_group" {
  name        = "slave_security_group"
  description = "Allow all internal traffic"
  vpc_id      = aws_default_vpc.default.id
  revoke_rules_on_delete = true
 
  # Allow communication between nodes in the VPC
  ingress {
    from_port   = "0"
    to_port     = "0"
    protocol    = "-1"
    self        = true
  }
 
  ingress {
      from_port   = "8443"
      to_port     = "8443"
      protocol    = "TCP"
  }
 
  egress {
    from_port   = "0"
    to_port     = "0"
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
 
  # Allow SSH traffic from VPN
  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "TCP"
    cidr_blocks = ["${aws_default_subnet.default.cidr_block}"]
  }
 
  lifecycle {
    ignore_changes = [ingress, egress]
  }
 
  tags = {
    name = "emr_test"
  }
}

resource "aws_key_pair" "emr_key_pair" {
  key_name   = "emr-key"
  public_key = file("cluster-key.pub")
}

provider "aws" {

    access_key = "ASIAQHDHPOITTHZRDAVT"
    secret_key = "07/MW4Rk+naTO2EjzM6vLEATqss23fCKYrHvqZLo"
    token = "FwoGZXIvYXdzEHoaDNjKPlKWzl892tpJDiLKAZk8ZToUwYqt+3kXstefLzkXW9eYgfWP40b7MEiu67d2hLYq+kYRo8wo+fcFvOYzYFsPQZSAnQWXBanppkPvld2mp04ht+usZnyt6zjQq4A52kf21QNGtIin3o+M1qbM+6Uvf6JMsLvR0zbw2ey/6fqzyHHvIp2tA2dAJoOVNC13qn3RMYAXUOOHNKK6SUqqb4lMjRocgJ5CpetcOejm7fWyAxYuNvnI6ANc9wTtBDGzNfKtlkVrx2h4L2QawpEbu4T4H2U/eG1IBkMo9p+b+AUyLUmHj/G36zV026fMRwa4XovVNfesNEZth4hQmlyZq+C/eF/8xlvoOgQO2ytCMQ=="
    region = "us-east-1"
}


resource "aws_s3_bucket" "bucketbigdataemr" {
  bucket = "bucketbigdataemr"

  region = "us-east-1"
 
  versioning {
    enabled = true
  }

  
}






resource "aws_iam_role" "glue_crawler_role" {
  name = "analytics_glue_crawler_role"

  assume_role_policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Action": "sts:AssumeRole",
      "Principal": {
        "Service": "glue.amazonaws.com"
      },
      "Effect": "Allow",
      "Sid": ""
    }
  ]
}
EOF
}




resource "aws_iam_role_policy" "glue_crawler_role_policy" {
  name = "analytics_glue_crawler_role_policy"
  role = aws_iam_role.glue_crawler_role.id

  policy = <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:*"
      ],
      "Resource": [
        "*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketLocation",
        "s3:ListBucket",
        "s3:GetBucketAcl",
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": [
        "arn:aws:s3:::analytics-product-data",
        "arn:aws:s3:::analytics-product-data/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": [
        "arn:aws:logs:*:*:/aws-glue/*"
      ]
    }
  ]
}
EOF
}



resource "aws_athena_database" "hoge" {
  name   = "users"
  bucket = aws_s3_bucket.bucketbigdataemr.bucket
}



resource "aws_glue_crawler" "product_crawler" {
  database_name = aws_athena_database.hoge.name
  name = "analytics-product-crawler"
  role = aws_iam_role.glue_crawler_role.arn

  schedule = "cron(0 0 * * ? *)"

  configuration = "{\"Version\": 1.0, \"CrawlerOutput\": { \"Partitions\": { \"AddOrUpdateBehavior\": \"InheritFromTable\" }, \"Tables\": {\"AddOrUpdateBehavior\": \"MergeNewColumns\" } } }"

  schema_change_policy {
    delete_behavior = "DELETE_FROM_DATABASE"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files"
  }
}




resource "aws_glue_catalog_table" "table_movies" {
  name          = "movies_table"
  database_name = aws_athena_database.hoge.name
  table_type    = "EXTERNAL_TABLE"
    owner = "owner"
    storage_descriptor {
        location        = "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie"
        input_format    = "org.apache.hadoop.mapred.TextInputFormat"
        output_format    = "org.apache.hadoop.mapred.TextInputFormat"
        #output_format   = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        compressed  = "false"
        number_of_buckets = -1    
        ser_de_info {
            name    = "movies"
            serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            parameters = {
                "field.delim" = ","
                "skip.header.line.count" = 1    # Skip file headers
            }
        }
        columns {
            name    = "movie_id"
            type    = "int"
        }
        columns {
            name    = "title"
            type    = "string"
        }
        columns {
            name    = "genres"
            type    = "string"
        }
    }
}


resource "aws_glue_catalog_table" "table_ratings" {
  name          = "ratings_table"
  database_name = aws_athena_database.hoge.name
  table_type    = "EXTERNAL_TABLE"
    owner = "owner"
    storage_descriptor {
        location        = "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/ratings"
        input_format    = "org.apache.hadoop.mapred.TextInputFormat"
        output_format   = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
        compressed  = "false"
        number_of_buckets = -1    
        ser_de_info {
            name    = "ratings"
            serialization_library = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
            parameters = {
                "field.delim" = ","
                "skip.header.line.count" = 1    # Skip file headers
            }
        }

        columns {
            name    = "user_id"
            type    = "int"
        }
        columns {
            name    = "movie_id"
            type    = "int"
        }
        columns {
            name    = "rating"
            type    = "float"
        }
        columns {
            name    = "timestamp"
            type    = "int"
        }
    }
}




resource "aws_athena_named_query" "one" {
  name      = "query_1"
  database  = aws_athena_database.hoge.name
  query     = "SELECT title FROM ${aws_glue_catalog_table.table_movies.name} WHERE genres LIKE '%Mystery%'"
}

resource "aws_athena_named_query" "two" {
  name      = "query_2"
  database  = aws_athena_database.hoge.name
  query     = "SELECT title FROM ${aws_glue_catalog_table.table_movies.name} WHERE title LIKE '%(2018)%'"
}
resource "aws_athena_named_query" "three" {
  name      = "query_3"
  database  = aws_athena_database.hoge.name
  query     = "SELECT title FROM ${aws_glue_catalog_table.table_movies.name} WHERE title LIKE '%(2016)%' OR title LIKE '%(2018)%'"
}
resource "aws_athena_named_query" "four" {
  name      = "query_4"
  database  = aws_athena_database.hoge.name
  query     = "SELECT DISTINCT title FROM ${aws_glue_catalog_table.table_movies.name} AS M JOIN ${aws_glue_catalog_table.table_ratings.name} AS R ON M.movie_id=R.movie_id WHERE rating=5.0"
}
resource "aws_athena_named_query" "five" {
  name      = "query_5"
  database  = aws_athena_database.hoge.name
  query     = "SELECT title FROM ${aws_glue_catalog_table.table_movies.name} AS M JOIN ${aws_glue_catalog_table.table_ratings.name} AS R ON M.movie_id=R.movie_id WHERE genres LIKE '%Fantasy%' AND genres LIKE '%Adventure%' AND rating=5.0"
}
resource "aws_athena_named_query" "six" {
  name      = "query_6"
  database  = aws_athena_database.hoge.name
  query     = "SELECT title FROM ${aws_glue_catalog_table.table_movies.name} AS M JOIN ${aws_glue_catalog_table.table_ratings.name} AS R ON M.movie_id=R.movie_id WHERE rating=1 OR rating=2 AND title LIKE '%(1995)%'"
}
resource "aws_athena_named_query" "seven" {
  name      = "query_7"
  database  = aws_athena_database.hoge.name
  query     = "SELECT DISTINCT title, rating FROM ${aws_glue_catalog_table.table_movies.name} AS M JOIN ${aws_glue_catalog_table.table_ratings.name} AS R ON M.movie_id=R.movie_id ORDER BY rating DESC"
}






 

resource "aws_emr_cluster" "emr-spark-cluster" {
   name = "EMR-cluster-example"
   release_label = "emr-5.13.0"
   applications = [ "Spark", "Hadoop"]
   
   
   ec2_attributes {
    
     instance_profile = aws_iam_instance_profile.emr_profile.arn
     key_name = aws_key_pair.emr_key_pair.key_name
     subnet_id = aws_default_subnet.default.id
     emr_managed_master_security_group = aws_security_group.master_security_group.id
     emr_managed_slave_security_group = aws_security_group.slave_security_group.id
   }
 
   core_instance_group {
     instance_type = "c5.xlarge"
     instance_count = 7
   }

   master_instance_group {
    instance_type = "c5.xlarge"
   }

   configurations_json = <<EOF
   [
     {
       "Classification": "spark",
       "Properties": {
         "maximizeResourceAllocation": "true"
       }
     }
   ]
 EOF


   log_uri = "s3://${aws_s3_bucket.bucketbigdataemr.bucket}"
 
   tags = {
     name = "EMR-cluster"
     role = "EMR_DefaultRole"
   }
 
  service_role = aws_iam_role.spark_cluster_iam_emr_service_role.arn


  step {
    
      action_on_failure = "CONTINUE"
      name              = "Create directory."
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        args = ["mkdir", "/home/hadoop/datasetS3/"]
      }
    }

  step {
    
      action_on_failure = "CONTINUE"
      name              = "Copy main.py script from s3"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        args = ["aws", "s3", "cp", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/main.py", "/home/hadoop/datasetS3/" ]
      }
    }

  step {
    
      action_on_failure = "CONTINUE"
      name              = "Copy methods.py script from s3"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        args = ["aws", "s3", "cp", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/methods.py", "/home/hadoop/datasetS3" ]
      }
    }

  step {
    
      action_on_failure = "CONTINUE"
      name              = "Copy baseline.py script from s3"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        args = ["aws", "s3", "cp", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/baseline.py", "/home/hadoop/datasetS3/" ]
      }
    }
    


  step {
      action_on_failure = "CONTINUE"
      name              = "Run Collaborative-Filtering."
      

      hadoop_jar_step {
       
        jar  = "command-runner.jar"
        args = ["spark-submit", "--deploy-mode", "client", "--master", "yarn" ,"--num-executors", "7", "--conf", "spark.dynamicAllocation.enabled=false","--py-files", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/zippo.zip", "/home/hadoop/datasetS3/main.py", "28"]
        
      }
    
  
  }

  step {
      action_on_failure = "CONTINUE"
      name              = "Run baseline."
      

      hadoop_jar_step {
       
        jar  = "command-runner.jar"
        args = ["spark-submit", "--deploy-mode", "client", "--master", "yarn" ,"--num-executors", "7", "--conf", "spark.dynamicAllocation.enabled=false","s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/baseline.py", "28"]
        
      }
    
  
  }

  
 

  step {

      action_on_failure = "CONTINUE"
      name              = "Copy csv file from s3 to movie folder."
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        args = ["aws", "s3", "cp", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movies.csv", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/"]
      }
    }



step {
    
      action_on_failure = "CONTINUE"
      name              = "Copy csv file from s3 to ratings folder."
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        args = ["aws", "s3", "cp", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/ratings.csv", "s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/ratings/" ]
      }
    }

step {
    
      action_on_failure = "CONTINUE"
      name              = "Execute query"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        #aws athena start-query-execution --query-string "select * from tbl01;" --result-configuration "OutputLocation=s3://ruan-athena-bucket/output/"     
        args = ["aws", "athena", "start-query-execution", "--query-string", "${aws_athena_named_query.one.query}", "--query-execution-context", "Database=${aws_athena_database.hoge.name}", "--result-configuration", "OutputLocation=s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/" ]
      }
    }
step {
    
      action_on_failure = "CONTINUE"
      name              = "Execute query"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        #aws athena start-query-execution --query-string "select * from tbl01;" --result-configuration "OutputLocation=s3://ruan-athena-bucket/output/"     
        args = ["aws", "athena", "start-query-execution", "--query-string", "${aws_athena_named_query.two.query}", "--query-execution-context", "Database=${aws_athena_database.hoge.name}", "--result-configuration", "OutputLocation=s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/" ]
      }
    }
step {
    
      action_on_failure = "CONTINUE"
      name              = "Execute query"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        #aws athena start-query-execution --query-string "select * from tbl01;" --result-configuration "OutputLocation=s3://ruan-athena-bucket/output/"     
        args = ["aws", "athena", "start-query-execution", "--query-string", "${aws_athena_named_query.three.query}", "--query-execution-context", "Database=${aws_athena_database.hoge.name}", "--result-configuration", "OutputLocation=s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/" ]
      }
    }
step {
    
      action_on_failure = "CONTINUE"
      name              = "Execute query"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        #aws athena start-query-execution --query-string "select * from tbl01;" --result-configuration "OutputLocation=s3://ruan-athena-bucket/output/"     
        args = ["aws", "athena", "start-query-execution", "--query-string", "${aws_athena_named_query.four.query}", "--query-execution-context", "Database=${aws_athena_database.hoge.name}", "--result-configuration", "OutputLocation=s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/" ]
      }
    }
step {
    
      action_on_failure = "CONTINUE"
      name              = "Execute query"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        #aws athena start-query-execution --query-string "select * from tbl01;" --result-configuration "OutputLocation=s3://ruan-athena-bucket/output/"     
        args = ["aws", "athena", "start-query-execution", "--query-string", "${aws_athena_named_query.five.query}", "--query-execution-context", "Database=${aws_athena_database.hoge.name}", "--result-configuration", "OutputLocation=s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/" ]
      }
    }
step {
    
      action_on_failure = "CONTINUE"
      name              = "Execute query"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        #aws athena start-query-execution --query-string "select * from tbl01;" --result-configuration "OutputLocation=s3://ruan-athena-bucket/output/"     
        args = ["aws", "athena", "start-query-execution", "--query-string", "${aws_athena_named_query.six.query}", "--query-execution-context", "Database=${aws_athena_database.hoge.name}", "--result-configuration", "OutputLocation=s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/" ]
      }
    }
step {
    
      action_on_failure = "CONTINUE"
      name              = "Execute query"
      

      hadoop_jar_step {
        jar  = "command-runner.jar"
        #aws athena start-query-execution --query-string "select * from tbl01;" --result-configuration "OutputLocation=s3://ruan-athena-bucket/output/"     
        args = ["aws", "athena", "start-query-execution", "--query-string", "${aws_athena_named_query.seven.query}", "--query-execution-context", "Database=${aws_athena_database.hoge.name}", "--result-configuration", "OutputLocation=s3://${aws_s3_bucket.bucketbigdataemr.bucket}/files/movie/" ]
      }
    }



}



#Scaricare l'ultima versione per eseguire questa parte di codice, altrimenti fileset non lo trova
resource "aws_s3_bucket_object" "bucketbigdataemr" {
  for_each = fileset(path.module, "files/*")
  bucket = aws_s3_bucket.bucketbigdataemr.bucket
  key = each.value
  source = "${path.module}/${each.value}"

  
}

