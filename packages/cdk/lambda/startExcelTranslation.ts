import { v4 as uuidv4 } from 'uuid';
import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient, PutItemCommand } from '@aws-sdk/client-dynamodb';
import {
  SFNClient,
  StartExecutionCommand,
} from '@aws-sdk/client-sfn';

const dynamoClient = new DynamoDBClient({});
const sfnClient = new SFNClient({});

const JOB_TABLE_NAME = process.env.JOB_TABLE_NAME || '';
const STATE_MACHINE_ARN = process.env.STATE_MACHINE_ARN || '';

interface TranslationRequest {
  s3Key: string;
  sourceLanguage: string;
  targetLanguage: string;
}

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  try {
    const req: TranslationRequest = JSON.parse(event.body!);
    const { s3Key, sourceLanguage, targetLanguage } = req;

    if (!s3Key) {
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
        },
        body: JSON.stringify({ error: 's3Key is required' }),
      };
    }

    // Generate job ID
    const jobId = uuidv4();
    const now = new Date().toISOString();
    // TTL: 24 hours from now
    const ttl = Math.floor(Date.now() / 1000) + 24 * 60 * 60;

    // Create job record in DynamoDB
    await dynamoClient.send(
      new PutItemCommand({
        TableName: JOB_TABLE_NAME,
        Item: {
          jobId: { S: jobId },
          status: { S: 'PENDING' },
          s3Key: { S: s3Key },
          sourceLanguage: { S: sourceLanguage || 'Japanese' },
          targetLanguage: { S: targetLanguage || 'English' },
          createdAt: { S: now },
          ttl: { N: ttl.toString() },
        },
      })
    );

    // Start Step Functions execution
    await sfnClient.send(
      new StartExecutionCommand({
        stateMachineArn: STATE_MACHINE_ARN,
        name: `excel-translate-${jobId}`,
        input: JSON.stringify({
          jobId,
          s3Key,
          sourceLanguage: sourceLanguage || 'Japanese',
          targetLanguage: targetLanguage || 'English',
        }),
      })
    );

    return {
      statusCode: 202,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
      body: JSON.stringify({
        jobId,
        status: 'PENDING',
        message: 'Translation job started',
      }),
    };
  } catch (error) {
    console.error('Error starting translation job:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
      body: JSON.stringify({ error: 'Failed to start translation job' }),
    };
  }
};
