import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';

const dynamoClient = new DynamoDBClient({});
const JOB_TABLE_NAME = process.env.JOB_TABLE_NAME || '';

export const handler = async (
  event: APIGatewayProxyEvent
): Promise<APIGatewayProxyResult> => {
  try {
    const jobId = event.pathParameters?.jobId;

    if (!jobId) {
      return {
        statusCode: 400,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
        },
        body: JSON.stringify({ error: 'jobId is required' }),
      };
    }

    // Get job record from DynamoDB
    const result = await dynamoClient.send(
      new GetItemCommand({
        TableName: JOB_TABLE_NAME,
        Key: {
          jobId: { S: jobId },
        },
      })
    );

    if (!result.Item) {
      return {
        statusCode: 404,
        headers: {
          'Content-Type': 'application/json',
          'Access-Control-Allow-Origin': '*',
        },
        body: JSON.stringify({ error: 'Job not found' }),
      };
    }

    const item = result.Item;
    const response: Record<string, unknown> = {
      jobId: item.jobId?.S,
      status: item.status?.S,
      createdAt: item.createdAt?.S,
    };

    // Include additional fields based on status
    if (item.status?.S === 'COMPLETED') {
      response.downloadUrl = item.downloadUrl?.S;
      response.outputS3Key = item.outputS3Key?.S;
      response.stats = item.stats?.S ? JSON.parse(item.stats.S) : null;
      response.completedAt = item.completedAt?.S;
    } else if (item.status?.S === 'FAILED') {
      response.error = item.error?.S;
      response.failedAt = item.failedAt?.S;
    } else if (item.status?.S === 'PROCESSING') {
      response.startedAt = item.startedAt?.S;
      // Include progress information
      if (item.progress?.S) {
        try {
          response.progress = JSON.parse(item.progress.S);
        } catch {
          response.progress = null;
        }
      }
    }

    return {
      statusCode: 200,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
      body: JSON.stringify(response),
    };
  } catch (error) {
    console.error('Error getting job status:', error);
    return {
      statusCode: 500,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
      },
      body: JSON.stringify({ error: 'Failed to get job status' }),
    };
  }
};
