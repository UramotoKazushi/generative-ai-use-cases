"""
Excel Translator Lambda Handler

Translates Excel files while preserving all formatting (fonts, borders, colors, etc.)
Supports async processing with DynamoDB job status tracking.
Features:
- Translation caching for duplicate text optimization
- Smart cell filtering (skip numbers, URLs, emails, dates)
- Support for both .xlsx and .xls formats
"""

import json
import os
import re
import time
import uuid
from datetime import datetime
from typing import Any

import boto3
from botocore.exceptions import ClientError
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from openpyxl import load_workbook
from openpyxl.cell.cell import Cell

# For .xls support
import xlrd
from openpyxl import Workbook

logger = Logger()

# Translation cache to avoid duplicate API calls
translation_cache: dict[str, str] = {}


def convert_xls_to_xlsx(xls_path: str, xlsx_path: str) -> None:
    """
    Convert .xls file to .xlsx format.
    Note: Some advanced formatting may not be preserved.
    """
    xls_book = xlrd.open_workbook(xls_path, formatting_info=False)
    xlsx_book = Workbook()

    # Remove default sheet
    if "Sheet" in xlsx_book.sheetnames:
        del xlsx_book["Sheet"]

    for sheet_idx in range(xls_book.nsheets):
        xls_sheet = xls_book.sheet_by_index(sheet_idx)
        xlsx_sheet = xlsx_book.create_sheet(title=xls_sheet.name)

        for row_idx in range(xls_sheet.nrows):
            for col_idx in range(xls_sheet.ncols):
                cell = xls_sheet.cell(row_idx, col_idx)
                value = cell.value

                # Handle different cell types
                if cell.ctype == xlrd.XL_CELL_DATE:
                    try:
                        value = xlrd.xldate.xldate_as_datetime(cell.value, xls_book.datemode)
                    except Exception:
                        pass
                elif cell.ctype == xlrd.XL_CELL_BOOLEAN:
                    value = bool(cell.value)
                elif cell.ctype == xlrd.XL_CELL_ERROR:
                    value = None

                xlsx_sheet.cell(row=row_idx + 1, column=col_idx + 1, value=value)

    xlsx_book.save(xlsx_path)
    logger.info(f"Converted .xls to .xlsx: {xls_path} -> {xlsx_path}")


def is_xls_file(filename: str) -> bool:
    """Check if file is .xls format (not .xlsx)"""
    return filename.lower().endswith(".xls") and not filename.lower().endswith(".xlsx")


s3_client = boto3.client("s3")
dynamodb_client = boto3.client("dynamodb")
bedrock_client = boto3.client("bedrock-runtime", region_name=os.environ.get("MODEL_REGION", "us-east-1"))

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-3-5-haiku-20241022-v1:0")
JOB_TABLE_NAME = os.environ.get("JOB_TABLE_NAME", "")


def update_job_status(job_id: str, status: str, **kwargs) -> None:
    """Update job status in DynamoDB"""
    if not JOB_TABLE_NAME or not job_id:
        return

    update_expr = "SET #status = :status"
    expr_names = {"#status": "status"}
    expr_values = {":status": {"S": status}}

    for key, value in kwargs.items():
        update_expr += f", {key} = :{key}"
        if isinstance(value, dict):
            expr_values[f":{key}"] = {"S": json.dumps(value)}
        else:
            expr_values[f":{key}"] = {"S": str(value)}

    try:
        dynamodb_client.update_item(
            TableName=JOB_TABLE_NAME,
            Key={"jobId": {"S": job_id}},
            UpdateExpression=update_expr,
            ExpressionAttributeNames=expr_names,
            ExpressionAttributeValues=expr_values,
        )
    except Exception as e:
        logger.warning(f"Failed to update job status: {e}")


def translate_text_with_retry(text: str, source_lang: str, target_lang: str, max_retries: int = 5) -> str:
    """Translate text using Amazon Bedrock with exponential backoff and caching"""
    if not text or not text.strip():
        return text

    # Check cache first
    cache_key = f"{source_lang}:{target_lang}:{text}"
    if cache_key in translation_cache:
        logger.debug(f"Cache hit for text: {text[:30]}...")
        return translation_cache[cache_key]

    prompt = f"""Translate the following {source_lang} text to {target_lang}.
Rules:
- Only output the translation, nothing else
- Preserve any numbers, special characters, and formatting
- If the text contains only numbers or symbols, return it as-is
- Keep line breaks if present

Text to translate:
{text}"""

    for attempt in range(max_retries):
        try:
            response = bedrock_client.converse(
                modelId=MODEL_ID,
                messages=[{"role": "user", "content": [{"text": prompt}]}],
                inferenceConfig={"maxTokens": 4096, "temperature": 0.1},
            )
            translated = response["output"]["message"]["content"][0]["text"].strip()
            # Store in cache
            translation_cache[cache_key] = translated
            return translated
        except ClientError as e:
            if e.response["Error"]["Code"] == "ThrottlingException":
                if attempt < max_retries - 1:
                    # Exponential backoff: 2, 4, 8, 16, 32 seconds
                    wait_time = (2 ** (attempt + 1)) + (time.time() % 1)  # Add jitter
                    logger.info(f"Throttled, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                    time.sleep(wait_time)
                    continue
            logger.warning(f"Translation failed for text: {text[:50]}... Error: {e}")
            return text
        except Exception as e:
            logger.warning(f"Translation failed for text: {text[:50]}... Error: {e}")
            return text

    return text  # Return original text if all retries fail


def batch_translate_texts(texts: list[tuple[str, Any]], source_lang: str, target_lang: str, job_id: str = None, base_translated: int = 0, total_translatable: int = 0) -> dict[Any, str]:
    """
    Batch translate multiple texts efficiently with caching.
    Returns a dict mapping original cell references to translated text.
    """
    # Filter out empty texts and non-string values
    translatable = [(ref, text) for ref, text in texts if text and isinstance(text, str) and text.strip()]

    if not translatable:
        return {}

    translations = {}
    texts_to_translate = []

    # Check cache first and separate cached vs uncached texts
    for ref, text in translatable:
        cache_key = f"{source_lang}:{target_lang}:{text}"
        if cache_key in translation_cache:
            translations[ref] = translation_cache[cache_key]
        else:
            texts_to_translate.append((ref, text))

    cache_hits = len(translations)
    if cache_hits > 0:
        logger.info(f"Cache hits: {cache_hits}/{len(translatable)} texts")

    if not texts_to_translate:
        return translations

    # Smaller batch size to avoid throttling
    BATCH_SIZE = 10
    max_retries = 5

    for i in range(0, len(texts_to_translate), BATCH_SIZE):
        batch = texts_to_translate[i : i + BATCH_SIZE]

        # Create a numbered list for batch translation
        numbered_texts = "\n".join([f"[{idx}] {text}" for idx, (ref, text) in enumerate(batch)])

        prompt = f"""Translate the following {source_lang} texts to {target_lang}.
Each text is prefixed with a number in brackets like [0], [1], etc.
Return translations in the same format, preserving the numbers.
Only output the translations, nothing else.
Preserve any numbers, special characters, and formatting within each text.
If a text contains only numbers or symbols, return it as-is.

Texts to translate:
{numbered_texts}"""

        success = False
        for attempt in range(max_retries):
            try:
                response = bedrock_client.converse(
                    modelId=MODEL_ID,
                    messages=[{"role": "user", "content": [{"text": prompt}]}],
                    inferenceConfig={"maxTokens": 8192, "temperature": 0.1},
                )
                result = response["output"]["message"]["content"][0]["text"].strip()

                # Parse the results
                for line in result.split("\n"):
                    line = line.strip()
                    if line.startswith("[") and "]" in line:
                        try:
                            bracket_end = line.index("]")
                            idx = int(line[1:bracket_end])
                            translated = line[bracket_end + 1 :].strip()
                            if 0 <= idx < len(batch):
                                ref, original_text = batch[idx]
                                translations[ref] = translated
                                # Store in cache
                                cache_key = f"{source_lang}:{target_lang}:{original_text}"
                                translation_cache[cache_key] = translated
                        except (ValueError, IndexError):
                            continue

                # Fill in any missing translations with individual calls
                for idx, (ref, text) in enumerate(batch):
                    if ref not in translations:
                        translations[ref] = translate_text_with_retry(text, source_lang, target_lang)

                success = True
                break

            except ClientError as e:
                if e.response["Error"]["Code"] == "ThrottlingException":
                    if attempt < max_retries - 1:
                        wait_time = (2 ** (attempt + 1)) + (time.time() % 1)
                        logger.info(f"Batch throttled, waiting {wait_time:.1f}s before retry {attempt + 2}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                logger.warning(f"Batch translation failed: {e}")
                break
            except Exception as e:
                logger.warning(f"Batch translation failed: {e}")
                break

        # Fallback to individual translations if batch failed
        if not success:
            logger.info(f"Falling back to individual translations for batch {i // BATCH_SIZE + 1}")
            for ref, text in batch:
                if ref not in translations:
                    translations[ref] = translate_text_with_retry(text, source_lang, target_lang)
                    # Small delay between individual calls
                    time.sleep(0.5)

        # Add delay between batches to avoid throttling
        if i + BATCH_SIZE < len(translatable):
            time.sleep(1.0)

        # Log progress
        batch_num = i // BATCH_SIZE + 1
        total_batches = (len(translatable) + BATCH_SIZE - 1) // BATCH_SIZE
        logger.info(f"Translated batch {batch_num}/{total_batches}")

        # Update progress in DynamoDB
        if job_id and total_translatable > 0:
            current_translated = base_translated + len(translations)
            update_job_status(
                job_id,
                "PROCESSING",
                progress=json.dumps({
                    "translated_cells": current_translated,
                    "total_translatable": total_translatable,
                    "percent": int(current_translated / total_translatable * 100),
                    "batch_progress": f"{batch_num}/{total_batches}",
                }),
            )

    return translations


def should_skip_text(text: str) -> bool:
    """
    Check if text should be skipped from translation.
    Returns True if the text doesn't need translation.
    """
    text = text.strip()

    # Empty or whitespace only
    if not text:
        return True

    # Numbers only (including decimals, negatives, percentages)
    if re.match(r'^-?[\d,]+\.?\d*%?$', text):
        return True

    # Date patterns (various formats)
    date_patterns = [
        r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$',  # 2024-01-15, 2024/01/15
        r'^\d{1,2}[-/]\d{1,2}[-/]\d{4}$',  # 01-15-2024, 01/15/2024
        r'^\d{1,2}[-/]\d{1,2}[-/]\d{2}$',  # 01-15-24, 01/15/24
        r'^\d{4}年\d{1,2}月\d{1,2}日$',     # 2024年1月15日
    ]
    for pattern in date_patterns:
        if re.match(pattern, text):
            return True

    # Time patterns
    if re.match(r'^\d{1,2}:\d{2}(:\d{2})?(\s*[APap][Mm])?$', text):
        return True

    # URLs
    if re.match(r'^https?://', text, re.IGNORECASE):
        return True

    # Email addresses
    if re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', text):
        return True

    # Phone numbers (various formats)
    if re.match(r'^[\d\s\-\+\(\)]+$', text) and len(re.sub(r'\D', '', text)) >= 7:
        return True

    # Single characters or symbols only
    if len(text) <= 2 and not any(c.isalpha() for c in text):
        return True

    # Currency values
    if re.match(r'^[$€£¥₩]\s*[\d,]+\.?\d*$', text):
        return True
    if re.match(r'^[\d,]+\.?\d*\s*[$€£¥₩円]$', text):
        return True

    return False


def is_translatable_cell(cell: Cell) -> bool:
    """Check if a cell contains translatable text"""
    if cell.value is None:
        return False
    if not isinstance(cell.value, str):
        return False
    if not cell.value.strip():
        return False
    # Skip cells that are formulas
    if str(cell.value).startswith("="):
        return False
    # Skip cells that don't need translation
    if should_skip_text(cell.value):
        return False
    return True


def translate_excel(input_path: str, output_path: str, source_lang: str, target_lang: str, job_id: str = None) -> dict:
    """
    Translate an Excel file while preserving all formatting.

    Returns statistics about the translation.
    """
    # Load workbook preserving all formatting
    wb = load_workbook(input_path)

    stats = {"total_cells": 0, "translated_cells": 0, "sheets_processed": 0, "total_sheets": len(wb.worksheets)}

    # First pass: count total translatable cells
    total_translatable = 0
    for sheet in wb.worksheets:
        for row in sheet.iter_rows():
            for cell in row:
                stats["total_cells"] += 1
                if is_translatable_cell(cell):
                    total_translatable += 1

    stats["total_translatable"] = total_translatable
    translated_count = 0

    for sheet_idx, sheet in enumerate(wb.worksheets):
        stats["sheets_processed"] = sheet_idx + 1
        stats["current_sheet"] = sheet.title

        # Update progress
        if job_id:
            update_job_status(
                job_id,
                "PROCESSING",
                progress=json.dumps({
                    "current_sheet": sheet.title,
                    "sheets_processed": sheet_idx + 1,
                    "total_sheets": len(wb.worksheets),
                    "translated_cells": translated_count,
                    "total_translatable": total_translatable,
                    "percent": int((translated_count / total_translatable * 100) if total_translatable > 0 else 0),
                }),
            )

        # Collect all cells that need translation
        cells_to_translate: list[tuple[str, str]] = []

        for row in sheet.iter_rows():
            for cell in row:
                if is_translatable_cell(cell):
                    # Use coordinate as reference
                    cells_to_translate.append((cell.coordinate, cell.value))

        # Batch translate all cells
        if cells_to_translate:
            translations = batch_translate_texts(cells_to_translate, source_lang, target_lang, job_id, translated_count, total_translatable)

            # Apply translations back to cells
            for coord, translated in translations.items():
                sheet[coord].value = translated
                translated_count += 1
                stats["translated_cells"] = translated_count

    # Save the translated workbook
    wb.save(output_path)

    return stats


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Lambda handler for Excel translation (async mode).

    Expected event format (async invocation from startExcelTranslation):
    {
        "jobId": "uuid",
        "s3Key": "uploads/xxx/file.xlsx",
        "sourceLanguage": "Japanese",
        "targetLanguage": "English"
    }

    Updates DynamoDB with job status as it processes.
    """
    job_id = event.get("jobId")
    s3_key = event.get("s3Key")
    source_lang = event.get("sourceLanguage", "Japanese")
    target_lang = event.get("targetLanguage", "English")

    # For backward compatibility, also check body
    if not s3_key:
        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)
        elif body:
            s3_key = body.get("s3Key")
            source_lang = body.get("sourceLanguage", "Japanese")
            target_lang = body.get("targetLanguage", "English")

    try:
        if not s3_key:
            error_msg = "s3Key is required"
            if job_id:
                update_job_status(job_id, "FAILED", error=error_msg, failedAt=datetime.utcnow().isoformat())
            return {"statusCode": 400, "body": json.dumps({"error": error_msg})}

        if not BUCKET_NAME:
            error_msg = "BUCKET_NAME not configured"
            if job_id:
                update_job_status(job_id, "FAILED", error=error_msg, failedAt=datetime.utcnow().isoformat())
            return {"statusCode": 500, "body": json.dumps({"error": error_msg})}

        # Update status to PROCESSING
        if job_id:
            update_job_status(job_id, "PROCESSING", startedAt=datetime.utcnow().isoformat())

        logger.info(f"Processing file: {s3_key}, {source_lang} -> {target_lang}, jobId: {job_id}")

        # Determine file type
        filename = os.path.basename(s3_key)
        is_xls = is_xls_file(filename)

        # Create temp file paths
        unique_id = uuid.uuid4()
        if is_xls:
            tmp_download = f"/tmp/input_{unique_id}.xls"
            tmp_input = f"/tmp/input_{unique_id}_converted.xlsx"
        else:
            tmp_download = f"/tmp/input_{unique_id}.xlsx"
            tmp_input = tmp_download
        tmp_output = f"/tmp/output_{unique_id}.xlsx"

        # Download file from S3
        s3_client.download_file(BUCKET_NAME, s3_key, tmp_download)
        logger.info(f"Downloaded file from S3: {s3_key}")

        # Convert .xls to .xlsx if necessary
        if is_xls:
            convert_xls_to_xlsx(tmp_download, tmp_input)
            logger.info(f"Converted .xls to .xlsx for processing")

        # Translate the Excel file
        stats = translate_excel(tmp_input, tmp_output, source_lang, target_lang, job_id)
        logger.info(f"Translation complete: {stats}")

        # Generate output S3 key (always output as .xlsx for compatibility)
        name, _ = os.path.splitext(filename)
        output_filename = f"{name}_translated.xlsx"
        output_s3_key = f"translated/{uuid.uuid4()}/{output_filename}"

        # Upload translated file to S3
        s3_client.upload_file(tmp_output, BUCKET_NAME, output_s3_key)
        logger.info(f"Uploaded translated file to S3: {output_s3_key}")

        # Clean up temp files
        if is_xls and os.path.exists(tmp_download):
            os.remove(tmp_download)
        if os.path.exists(tmp_input):
            os.remove(tmp_input)
        if os.path.exists(tmp_output):
            os.remove(tmp_output)

        # Generate presigned URL for download
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET_NAME, "Key": output_s3_key},
            ExpiresIn=3600,  # 1 hour
        )

        # Update job status to COMPLETED
        if job_id:
            update_job_status(
                job_id,
                "COMPLETED",
                outputS3Key=output_s3_key,
                downloadUrl=presigned_url,
                stats=stats,
                completedAt=datetime.utcnow().isoformat(),
            )

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(
                {
                    "jobId": job_id,
                    "outputS3Key": output_s3_key,
                    "downloadUrl": presigned_url,
                    "stats": stats,
                }
            ),
        }

    except Exception as e:
        logger.exception("Error processing Excel translation")
        error_msg = str(e)
        if job_id:
            update_job_status(job_id, "FAILED", error=error_msg, failedAt=datetime.utcnow().isoformat())
        return {"statusCode": 500, "body": json.dumps({"error": error_msg})}
