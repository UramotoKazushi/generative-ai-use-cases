import { APIGatewayProxyEvent, APIGatewayProxyResult } from 'aws-lambda';
import { DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb';

const dynamoClient = new DynamoDBClient({});
const JOB_TABLE_NAME = process.env.JOB_TABLE_NAME || '';

// Convert camelCase stats to snake_case for frontend compatibility
function convertStats(stats: Record<string, unknown>) {
  return {
    total_cells: stats.totalCells,
    translatable_cells: stats.translatableCells,
    translated_cells: stats.translatedCells,
    sheets_processed: stats.sheetsProcessed,
    unique_texts: stats.uniqueTexts,
    batch_count: stats.batchCount,
  };
}

// Convert progress info for frontend
function convertProgress(progress: Record<string, unknown>) {
  const result: Record<string, unknown> = {
    percent: progress.percent,
    phase: progress.phase,
  };

  // Map phase to status message and calculate progress details
  if (progress.phase === 'prepared') {
    result.total_translatable = progress.uniqueTexts;
    result.batch_count = progress.batches;
  } else if (progress.phase === 'translating') {
    result.completed_batches = progress.completedBatches;
    result.total_batches = progress.totalBatches;
    result.batch_progress = `${progress.completedBatches}/${progress.totalBatches}`;

    // Include time estimates if available
    if (progress.elapsedSeconds !== undefined) {
      result.elapsed_seconds = progress.elapsedSeconds;
    }
    if (progress.estimatedRemainingSeconds !== undefined) {
      result.estimated_remaining_seconds = progress.estimatedRemainingSeconds;
    }
  } else if (progress.phase === 'merging') {
    result.percent = progress.percent || 90;
  }

  return result;
}

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
    const status = item.status?.S;

    const response: Record<string, unknown> = {
      jobId: item.jobId?.S,
      status: status,
      createdAt: item.createdAt?.S,
    };

    // Include progress for all in-progress states
    if (item.progress?.S) {
      try {
        const rawProgress = JSON.parse(item.progress.S);
        response.progress = convertProgress(rawProgress);
      } catch {
        response.progress = null;
      }
    }

    // Include additional fields based on status
    if (status === 'COMPLETED') {
      response.downloadUrl = item.downloadUrl?.S;
      response.outputS3Key = item.outputS3Key?.S;
      response.completedAt = item.completedAt?.S;
      if (item.stats?.S) {
        try {
          const rawStats = JSON.parse(item.stats.S);
          response.stats = convertStats(rawStats);
        } catch {
          response.stats = null;
        }
      }
    } else if (status === 'FAILED') {
      response.error = item.error?.S;
      response.failedAt = item.failedAt?.S;
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
