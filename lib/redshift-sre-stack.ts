import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as fs from 'fs';
import * as path from 'path';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
// import * as cfn from 'aws-cdk-lib/aws-cloudformation';

export class RedshiftSreStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Load environment variables from config file
    const configPath = path.join(__dirname, '../config.json');
    const config = JSON.parse(fs.readFileSync(configPath, 'utf8'));

    // Create the Lambda function
    const redshiftLambda = new lambda.Function(this, 'RedshiftLambda', {
      runtime: lambda.Runtime.PYTHON_3_9,
      handler: 'index.handler',
      code: lambda.Code.fromAsset('service'),
      timeout: cdk.Duration.minutes(5), // Increase timeout duration
      environment: {
        'DB_NAME': config.DB_NAME,
        'DB_USER': config.DB_USER,
        'DB_PASSWORD': config.DB_PASSWORD,
        'DB_HOST': config.DB_HOST,
        'DB_PORT': config.DB_PORT,
        'WORKGROUP_NAME': config.WORKGROUP_NAME,
        'LOG_GROUP_NAME': config.LOG_GROUP_NAME,
        'LOG_STREAM_NAME': config.LOG_STREAM_NAME,
        'ELAPSED_TIME': config.ELAPSED_TIME.toString() // Add this line
      }
    });

    // Add IAM policy to allow Lambda to connect to Redshift and put CloudWatch metrics
    redshiftLambda.addToRolePolicy(new iam.PolicyStatement({
      actions: [
        'redshift-data:ExecuteStatement',
        'redshift-data:GetStatementResult',
        'redshift-data:DescribeStatement', // Added action
        'redshift-serverless:GetCredentials',
        'cloudwatch:PutMetricData' // Add this line
      ],
      resources: ['*']
    }));

    // Create an EventBridge rule to trigger the Lambda based on the schedule interval from the config
    const rule = new events.Rule(this, 'ScheduleRule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(config.SCHEDULE_INTERVAL)), // Use SCHEDULE_INTERVAL from config
    });

    rule.addTarget(new targets.LambdaFunction(redshiftLambda));

    // Create a CloudWatch dashboard with a log widget
    const dashboard = new cloudwatch.Dashboard(this, 'RedshiftDashboard', {
      dashboardName: 'RedshiftQueryResults'
    });

    dashboard.addWidgets(
      new cloudwatch.LogQueryWidget({
        logGroupNames: [config.LOG_GROUP_NAME],
        view: cloudwatch.LogQueryVisualizationType.TABLE,
        title: 'Redshift Query Results',
        queryString: `parse @message '[{"stringValue": "*"}, {"isNull": *}, {"longValue": *}, {"stringValue": "*"}, {"stringValue": "*"}, {"longValue": *}, {"stringValue": "*"}, {"stringValue": "*"}, {"longValue": *}, {"longValue": *}, {"longValue": *}]'  
    as timestamp, _, session_id, iam_role, environment, execution_time, query_type, query_text, elapsed_time, queue_time, lock_wait_time  
| display timestamp, session_id, iam_role, environment, execution_time, query_type, query_text, elapsed_time, queue_time, lock_wait_time  
| sort timestamp desc`,
        width: 24 // Expand the widget to be wider
      }),
      new cloudwatch.GraphWidget({
        title: 'Rows Exceeding Elapsed Time Threshold',
        left: [
          new cloudwatch.Metric({
            namespace: 'RedshiftQueryMetrics',
            metricName: 'RowsExceedingElapsedTimeThreshold',
            dimensionsMap: {
              LogGroupName: config.LOG_GROUP_NAME,
              LogStreamName: config.LOG_STREAM_NAME
            }
          })
        ],
        width: 24 // Expand the widget to be wider
      })
    );
  }
}
