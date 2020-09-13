[![Build status](https://dev.azure.com/SandranaHemanth/SparkSqlManager/_apis/build/status/Spark%20Sql%20Parser)](https://dev.azure.com/SandranaHemanth/SparkSqlManager/_build/latest?definitionId=1)
# Build and Deploy Spark SQL Tables.
  - Build and Deploy Spark SQL tables incrementally. Check for syntax errors before checking in the code to master and Deploy the changes using Continuous Deployment. This project aims to create a way to deploy spark sql tables using CI/CD and focus just on table schema changes rather than how to deploy the changes.

# Spark Sql Project
  - Create a spark sql project which contains details about project files like Schema and Table scripts.
  - Build this project using BuildSql.jar which outputs Build Artifact that will be passed to DeploymentManager to deploy the changes.
  - Much like sql project deployments, here too we have Post and Pre Deplyoment Scripts. These scripts are Scala Notebooks. The Pre deployment and Post deployment notebooks should be executed before and after executing the deployment respectively.

# Build
## Features

  - SqlBuild jar helps building the spark sql project.
  - Build typically checks for syntax errors.
  - Once the build succeeds, it will create a build artifact which can be used to Deloy the changes ( by invoking Deployment Manager) 
## How to build

  - BuildSql project creates BuildSql.jar file.
  - Use BuildSql.jar like an executable to build spark sql project.
  - Run the jar by passing .sparkSql project file as command line arguments.
  - Build Artifact is generated once build succeeds. You can find this artifact in bin folder created in project root directory.

# Deploy
## Features

  - Currently Supports Delta table Deplyoment.
  - Execute Pre and Post Deployment Notebooks (typically to change anything manual or create some master data).
## How to deploy

  - Execute the DeploymentManager jar on the spark cluster by passing output.json (build artifact) as jar argument.
  - Make Sure to execute Pre and Post Deployment Notebooks on the cluster before and after executing the DeploymentManager jar respectively.
