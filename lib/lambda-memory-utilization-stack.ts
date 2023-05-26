import { Duration, RemovalPolicy, Stack, StackProps} from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { aws_iam as iam } from 'aws-cdk-lib';
import { aws_kinesisfirehose as kinesisfirehose } from 'aws-cdk-lib'
import { aws_logs as logs } from 'aws-cdk-lib'
import { aws_s3 as s3 } from 'aws-cdk-lib';
import { aws_sqs as sqs } from 'aws-cdk-lib';
import { aws_stepfunctions as sfn } from 'aws-cdk-lib';
import { aws_stepfunctions_tasks as tasks } from 'aws-cdk-lib';
import { aws_lambda as lambda } from 'aws-cdk-lib';
import { aws_lambda_event_sources as lambdaEventSources } from 'aws-cdk-lib';
import * as lambdaPython from '@aws-cdk/aws-lambda-python-alpha';
import * as glue from '@aws-cdk/aws-glue-alpha';
import * as path from 'path';

import { columns, partitions } from "../schema/schema"

export interface LambdaMemoryUtilizationStackProps extends StackProps {}

export class LambdaMemoryUtilizationStack extends Stack {
  constructor(scope: Construct, id: string, props?: LambdaMemoryUtilizationStackProps) {
    super(scope, id, props);

    const dataBucket = new s3.Bucket(this, 'DataBucket', {
      bucketName: `lambda-memory-utilization-${this.account}`,
      encryption: s3.BucketEncryption.S3_MANAGED,
    })

    const logGroupQueue = new sqs.Queue(this, 'LogGroupQueue', {
      queueName: 'lambda-log-groups',
      retentionPeriod: Duration.days(14),
      visibilityTimeout: Duration.minutes(3),
      receiveMessageWaitTime: Duration.seconds(20),
    });

    const logGroupLoaderLambda = new lambdaPython.PythonFunction(this, 'LogGroupLoaderLambda', {
      functionName: `lambda-log-group-loader`,
      description: 'Loads Lambda log groups into SQS queue',
      entry: path.join(__dirname, '..', 'lambdas'), 
      runtime: lambda.Runtime.PYTHON_3_12, 
      index: 'list_lambda_functions.py',
      handler: 'handler',
      timeout: Duration.minutes(15),
      memorySize: 256,
      retryAttempts: 0,
      architecture: lambda.Architecture.ARM_64,
      environment: {
        SQS_QUEUE_URL: logGroupQueue.queueUrl,
      }
    });
    logGroupLoaderLambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: [
        'account:ListRegions',
        'lambda:ListFunctions'
      ],
    }));
    logGroupQueue.grantSendMessages(logGroupLoaderLambda);

    const {glueDatabase, glueTable} = this.createGlueResources(dataBucket);
    const firehoseStream = this.createFirehoseResources(dataBucket, glueDatabase, glueTable);
    this.createStepFunction(logGroupLoaderLambda);

    const logGroupWorkerLambda = new lambdaPython.PythonFunction(this, 'LogGroupWorkerLambda', {
      functionName: `lambda-log-group-worker`,
      description: 'Polls SQS queue and submits query to CloudWatch Logs Insights',
      entry: path.join(__dirname, '..', 'lambdas'), 
      runtime: lambda.Runtime.PYTHON_3_12, 
      index: 'query_logs_insights.py',
      handler: 'handler',
      timeout: Duration.minutes(3),
      memorySize: 128,
      retryAttempts: 0,
      architecture: lambda.Architecture.ARM_64,
      reservedConcurrentExecutions: 10, // don't want to overwhelm CloudWatch Logs Insights
      environment: { FIREHOSE_STREAM: firehoseStream.deliveryStreamName || ''}
    });
    logGroupWorkerLambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: ['*'],
      actions: [
        'logs:StartQuery',
        'logs:GetQueryResults',
      ],
    }));
    logGroupWorkerLambda.addEventSource(new lambdaEventSources.SqsEventSource(logGroupQueue, { batchSize: 1 }))
    logGroupWorkerLambda.role?.addToPrincipalPolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      resources: [firehoseStream.attrArn],
      actions: ['firehose:PutRecord*'],
    }));
  }

  private createGlueResources(dataBucket: s3.Bucket) {
    const glueDatabase = new glue.Database(this, 'GlueDatabase', {
      databaseName: 'lambda_memory_utilization',
      description: 'Monitor Lambda memory utilization'
    });

    const glueTable = new glue.S3Table(this, 'GlueTable', {
        database: glueDatabase,
        bucket: dataBucket,
        tableName: 'lambda_memory_utilization',
        columns: columns,
        partitionKeys: partitions,
        dataFormat: glue.DataFormat.PARQUET,
        enablePartitionFiltering: true,
    });

    return {glueDatabase, glueTable};
  }

  private createFirehoseResources(dataBucket: s3.Bucket, glueDatabase: glue.Database, glueTable: glue.S3Table): kinesisfirehose.CfnDeliveryStream {
    const logGroup = new logs.LogGroup(this, 'FirehoseLogGroup', {
      logGroupName: `/aws/firehose/lambda-memory-utilization`,
      removalPolicy: RemovalPolicy.DESTROY,
    });
    const logStream = new logs.LogStream(this, 'FirehoseLogStream', {
        logGroup: logGroup,
        removalPolicy: RemovalPolicy.DESTROY,
      });
    const firehoseRole = new iam.Role(this, 'FirehoseRole', {
        roleName: 'firehose-lambda-memory-utilization',
        assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com')
    });
    dataBucket.grantReadWrite(firehoseRole);
    logGroup.grantWrite(firehoseRole);
    firehoseRole.addToPrincipalPolicy(new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        resources: [glueDatabase.databaseArn, glueDatabase.catalogArn, glueTable.tableArn],
        actions: ['glue:GetTableVersions'],
    }));

    const firehoseStream = new kinesisfirehose.CfnDeliveryStream(this, 'Firehose', {
      deliveryStreamName: 'lambda-memory-utilization',
      extendedS3DestinationConfiguration: {
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: logGroup.logGroupName,
          logStreamName: logStream.logStreamName,
        },
        bucketArn: dataBucket.bucketArn,
        roleArn: firehoseRole.roleArn,
        prefix: 'metric_collection_date=!{partitionKeyFromQuery:metric_collection_date}/',
        errorOutputPrefix: 'errors/!{firehose:error-output-type}/!{timestamp:yyyy}/!{timestamp:mm}/!{timestamp:dd}',
        bufferingHints: {
          intervalInSeconds: 60,
          sizeInMBs: 128,
        },
        dynamicPartitioningConfiguration: {
          enabled: true,
        },
        dataFormatConversionConfiguration: {
          enabled: true,
          inputFormatConfiguration: {deserializer: { openXJsonSerDe: {} }},
          outputFormatConfiguration: {serializer: { parquetSerDe: {} }},
          schemaConfiguration: {
              databaseName: glueDatabase.databaseName,
              tableName: glueTable.tableName,
              roleArn: firehoseRole.roleArn,
          }
        },
        processingConfiguration: {
          enabled: true,
          processors: [
            {
              type: 'MetadataExtraction',
              parameters: [
                {
                  parameterName: 'MetadataExtractionQuery',
                  parameterValue: '{metric_collection_date: .metric_collection_date}',
                },
                {
                  parameterName: 'JsonParsingEngine',
                  parameterValue: 'JQ-1.6',
                },
              ],
            },
          ],
        },
      },
    });
    // eventually consistency, need to wait a sec for the role to be ready
    firehoseStream.node.addDependency(firehoseRole);
    return firehoseStream;
  }

  private createStepFunction(logGroupLoaderLambda: lambda.Function) {    
    const sfnSuccess = new sfn.Succeed(this, 'Success');

    const loaderInvoker = new tasks.LambdaInvoke(this, 'Load Lambda log groups into SQS queue', {
      lambdaFunction: logGroupLoaderLambda,
      inputPath: '$',
      outputPath: '$.Payload',
    });

    const nextTokenChoice = new sfn.Choice(this, 'Are there more Lambda log groups to process?');
    nextTokenChoice.when(sfn.Condition.isPresent('$.next_token'), loaderInvoker);
    nextTokenChoice.otherwise(sfnSuccess);
    
    const definition = loaderInvoker.next(nextTokenChoice);
    const lambdaMemoryUtilizationSfn = new sfn.StateMachine(this, 'StepFunction', {
      timeout: Duration.days(30),
      definition: definition,
      stateMachineName: `lambda-memory-utilization`,
    });

    return lambdaMemoryUtilizationSfn;
  }
}
