"""
Excel Translation - Batch Translate Handler for Step Functions

This Lambda:
1. Loads a batch of texts from S3
2. Translates using Bedrock
3. Stores translations in S3
4. Returns translation key
"""

import json
import os
import time
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()

s3_client = boto3.client("s3")
bedrock_client = boto3.client("bedrock-runtime", region_name=os.environ.get("MODEL_REGION", "us-east-1"))
dynamodb_client = boto3.client("dynamodb")

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-3-5-haiku-20241022-v1:0")
JOB_TABLE_NAME = os.environ.get("JOB_TABLE_NAME", "")


def translate_texts_batch(
    texts: list[str],
    source_lang: str,
    target_lang: str,
    max_retries: int = 5
) -> dict[str, str]:
    """
    Translate a batch of texts using Bedrock with JSON format.
    Returns a dict mapping original text to translated text.
    """
    if not texts:
        return {}

    translations = {}

    # Create JSON input for structured translation
    input_data = [{"id": i, "text": text} for i, text in enumerate(texts)]

    prompt = f"""Translate the following {source_lang} texts to {target_lang}.

IMPORTANT RULES:
- Return ONLY a valid JSON array with translations
- Each item must have "id" (same as input) and "translation" fields
- Preserve numbers, special characters, and formatting
- If text contains only numbers/symbols, return as-is

Input:
{json.dumps(input_data, ensure_ascii=False)}

Output format (JSON array only, no other text):
[{{"id": 0, "translation": "..."}}, {{"id": 1, "translation": "..."}}, ...]"""

    for attempt in range(max_retries):
        try:
            response = bedrock_client.converse(
                modelId=MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": 16384, "temperature": 0.1},
            )
            result = response["output"]["message"]["content"][0]["text"].strip()

            # Parse JSON response
            if result.startswith("```"):
                result = result.split("```")[1]
                if result.startswith("json"):
                    result = result[4:]
                result = result.strip()

            try:
                parsed = json.loads(result)
                for item in parsed:
                    idx = item.get("id")
                    translated = item.get("translation", "")
                    if idx is not None and 0 <= idx < len(texts):
                        translations[texts[idx]] = translated
            except json.JSONDecodeError:
                # Fallback: try line-by-line parsing
                logger.warning("JSON parse failed, attempting line-by-line parse")
                for line in result.split("\n"):
                    line = line.strip()
                    if '"id"' in line and '"translation"' in line:
                        try:
                            item = json.loads(line.rstrip(","))
                            idx = item.get("id")
                            translated = item.get("translation", "")
                            if idx is not None and 0 <= idx < len(texts):
                                translations[texts[idx]] = translated
                        except:
                            continue

            logger.info(f"Translated {len(translations)}/{len(texts)} texts")
            return translations

        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                if attempt < max_retries - 1:
                    wait_time = (2 ** (attempt + 1)) + (time.time() % 1)
                    logger.info(f"Throttled, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                    time.sleep(wait_time)
                    continue
            logger.error(f"Bedrock API error: {e}")
            raise
        except Exception as e:
            logger.error(f"Translation error: {e}")
            raise

    return translations


def update_batch_progress(job_id: str, total_batches: int, start_time: str | None = None) -> None:
    """
    Atomically increment completed batch count and update progress.
    Uses DynamoDB atomic counter for parallel batch tracking.
    """
    if not JOB_TABLE_NAME or not job_id:
        return

    try:
        # Atomically increment completedBatches counter
        response = dynamodb_client.update_item(
            TableName=JOB_TABLE_NAME,
            Key={"jobId": {"S": job_id}},
            UpdateExpression="SET completedBatches = if_not_exists(completedBatches, :zero) + :inc",
            ExpressionAttributeValues={
                ":inc": {"N": "1"},
                ":zero": {"N": "0"},
            },
            ReturnValues="UPDATED_NEW"
        )

        completed = int(response["Attributes"]["completedBatches"]["N"])

        # Calculate progress: 5% (prepare) + (completed/total * 85%) during translation
        progress_percent = 5 + int((completed / total_batches) * 85)

        # Update progress info
        progress_data = {
            "phase": "translating",
            "completedBatches": completed,
            "totalBatches": total_batches,
            "percent": progress_percent,
        }

        # Add elapsed time and estimate if we have start time
        if start_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
                elapsed_seconds = (datetime.now(start_dt.tzinfo) - start_dt).total_seconds()
                progress_data["elapsedSeconds"] = int(elapsed_seconds)

                if completed > 0:
                    seconds_per_batch = elapsed_seconds / completed
                    remaining_batches = total_batches - completed
                    estimated_remaining = int(seconds_per_batch * remaining_batches)
                    progress_data["estimatedRemainingSeconds"] = estimated_remaining
            except Exception as e:
                logger.warning(f"Failed to calculate time estimates: {e}")

        dynamodb_client.update_item(
            TableName=JOB_TABLE_NAME,
            Key={"jobId": {"S": job_id}},
            UpdateExpression="SET progress = :progress",
            ExpressionAttributeValues={
                ":progress": {"S": json.dumps(progress_data)},
            },
        )

        logger.info(f"Updated progress: {completed}/{total_batches} batches ({progress_percent}%)")

    except Exception as e:
        logger.warning(f"Failed to update batch progress: {e}")


def translate_single_text(text: str, source_lang: str, target_lang: str, max_retries: int = 3) -> str:
    """Translate a single text as fallback"""
    if not text or not text.strip():
        return text

    prompt = f"""Translate to {target_lang}. Output only the translation:
{text}"""

    for attempt in range(max_retries):
        try:
            response = bedrock_client.converse(
                modelId=MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": 1024, "temperature": 0.1},
            )
            return response["output"]["message"]["content"][0]["text"].strip()
        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException" and attempt < max_retries - 1:
                time.sleep(2 ** (attempt + 1))
                continue
            return text
        except:
            return text

    return text


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Translate batch handler for Step Functions Map state.

    Input (from Map state):
    {
        "batchId": 0,
        "batchKey": "excel-work/jobId/batch_0.json",
        "textCount": 100,
        "jobId": "uuid",
        "sourceLanguage": "Japanese",
        "targetLanguage": "English",
        "totalBatches": 32,
        "startTime": "2024-01-15T10:00:00Z"
    }

    Output:
    {
        "batchId": 0,
        "translationKey": "excel-work/jobId/translation_0.json",
        "translatedCount": 100,
        "success": true
    }
    """
    batch_id = event.get("batchId")
    batch_key = event.get("batchKey")
    job_id = event.get("jobId")
    source_lang = event.get("sourceLanguage", "Japanese")
    target_lang = event.get("targetLanguage", "English")
    total_batches = event.get("totalBatches", 1)
    start_time = event.get("startTime")

    logger.info(f"Translating batch {batch_id} for job {job_id}")

    # Load batch from S3
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=batch_key)
    batch_data = json.loads(response["Body"].read().decode("utf-8"))
    texts = batch_data.get("texts", [])

    logger.info(f"Loaded {len(texts)} texts from batch {batch_id}")

    # Translate batch
    translations = translate_texts_batch(texts, source_lang, target_lang)

    # Handle any missing translations with individual fallback
    missing = [t for t in texts if t not in translations]
    if missing:
        logger.info(f"Retrying {len(missing)} missing translations individually")
        for text in missing:
            translated = translate_single_text(text, source_lang, target_lang)
            translations[text] = translated

    # Store translations in S3
    work_prefix = f"excel-work/{job_id}"
    translation_key = f"{work_prefix}/translation_{batch_id}.json"

    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=translation_key,
        Body=json.dumps({"translations": translations}, ensure_ascii=False),
        ContentType="application/json"
    )

    logger.info(f"Stored {len(translations)} translations for batch {batch_id}")

    # Update progress after batch completion
    update_batch_progress(job_id, total_batches, start_time)

    return {
        "batchId": batch_id,
        "translationKey": translation_key,
        "translatedCount": len(translations),
        "success": True
    }
