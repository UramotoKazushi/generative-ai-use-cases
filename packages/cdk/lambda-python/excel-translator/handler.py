"""
Excel Translator Lambda Handler

Translates Excel files while preserving all formatting (fonts, borders, colors, etc.)
"""

import json
import os
import uuid
from typing import Any

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from openpyxl import load_workbook
from openpyxl.cell.cell import Cell

logger = Logger()

s3_client = boto3.client("s3")
bedrock_client = boto3.client("bedrock-runtime", region_name=os.environ.get("MODEL_REGION", "us-east-1"))

BUCKET_NAME = os.environ.get("BUCKET_NAME", "")
MODEL_ID = os.environ.get("MODEL_ID", "us.anthropic.claude-3-5-haiku-20241022-v1:0")


def translate_text(text: str, source_lang: str, target_lang: str) -> str:
    """Translate text using Amazon Bedrock"""
    if not text or not text.strip():
        return text

    prompt = f"""Translate the following {source_lang} text to {target_lang}.
Rules:
- Only output the translation, nothing else
- Preserve any numbers, special characters, and formatting
- If the text contains only numbers or symbols, return it as-is
- Keep line breaks if present

Text to translate:
{text}"""

    try:
        response = bedrock_client.converse(
            modelId=MODEL_ID,
            messages=[{"role": "user", "content": [{"text": prompt}]}],
            inferenceConfig={"maxTokens": 4096, "temperature": 0.1},
        )
        return response["output"]["message"]["content"][0]["text"].strip()
    except Exception as e:
        logger.warning(f"Translation failed for text: {text[:50]}... Error: {e}")
        return text  # Return original text if translation fails


def batch_translate_texts(texts: list[tuple[str, Any]], source_lang: str, target_lang: str) -> dict[Any, str]:
    """
    Batch translate multiple texts efficiently.
    Returns a dict mapping original cell references to translated text.
    """
    # Filter out empty texts and non-string values
    translatable = [(ref, text) for ref, text in texts if text and isinstance(text, str) and text.strip()]

    if not translatable:
        return {}

    # For efficiency, combine texts and translate in batches
    BATCH_SIZE = 20
    translations = {}

    for i in range(0, len(translatable), BATCH_SIZE):
        batch = translatable[i : i + BATCH_SIZE]

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
                            ref, _ = batch[idx]
                            translations[ref] = translated
                    except (ValueError, IndexError):
                        continue

            # Fill in any missing translations with individual calls
            for idx, (ref, text) in enumerate(batch):
                if ref not in translations:
                    translations[ref] = translate_text(text, source_lang, target_lang)

        except Exception as e:
            logger.warning(f"Batch translation failed: {e}, falling back to individual translations")
            for ref, text in batch:
                translations[ref] = translate_text(text, source_lang, target_lang)

    return translations


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
    return True


def translate_excel(input_path: str, output_path: str, source_lang: str, target_lang: str) -> dict:
    """
    Translate an Excel file while preserving all formatting.

    Returns statistics about the translation.
    """
    # Load workbook preserving all formatting
    wb = load_workbook(input_path)

    stats = {"total_cells": 0, "translated_cells": 0, "sheets_processed": 0}

    for sheet in wb.worksheets:
        stats["sheets_processed"] += 1

        # Collect all cells that need translation
        cells_to_translate: list[tuple[str, str]] = []

        for row in sheet.iter_rows():
            for cell in row:
                stats["total_cells"] += 1
                if is_translatable_cell(cell):
                    # Use coordinate as reference
                    cells_to_translate.append((cell.coordinate, cell.value))

        # Batch translate all cells
        if cells_to_translate:
            translations = batch_translate_texts(cells_to_translate, source_lang, target_lang)

            # Apply translations back to cells
            for coord, translated in translations.items():
                sheet[coord].value = translated
                stats["translated_cells"] += 1

    # Save the translated workbook
    wb.save(output_path)

    return stats


@logger.inject_lambda_context
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    """
    Lambda handler for Excel translation.

    Expected event format:
    {
        "s3Key": "uploads/xxx/file.xlsx",
        "sourceLanguage": "Japanese",
        "targetLanguage": "English"
    }

    Returns:
    {
        "statusCode": 200,
        "body": {
            "outputS3Key": "translated/xxx/file_translated.xlsx",
            "stats": {...}
        }
    }
    """
    try:
        # Parse input
        body = event.get("body")
        if isinstance(body, str):
            body = json.loads(body)
        else:
            body = event

        s3_key = body.get("s3Key")
        source_lang = body.get("sourceLanguage", "Japanese")
        target_lang = body.get("targetLanguage", "English")

        if not s3_key:
            return {"statusCode": 400, "body": json.dumps({"error": "s3Key is required"})}

        if not BUCKET_NAME:
            return {"statusCode": 500, "body": json.dumps({"error": "BUCKET_NAME not configured"})}

        logger.info(f"Processing file: {s3_key}, {source_lang} -> {target_lang}")

        # Create temp file paths
        tmp_input = f"/tmp/input_{uuid.uuid4()}.xlsx"
        tmp_output = f"/tmp/output_{uuid.uuid4()}.xlsx"

        # Download file from S3
        s3_client.download_file(BUCKET_NAME, s3_key, tmp_input)
        logger.info(f"Downloaded file from S3: {s3_key}")

        # Translate the Excel file
        stats = translate_excel(tmp_input, tmp_output, source_lang, target_lang)
        logger.info(f"Translation complete: {stats}")

        # Generate output S3 key
        filename = os.path.basename(s3_key)
        name, ext = os.path.splitext(filename)
        output_filename = f"{name}_translated{ext}"
        output_s3_key = f"translated/{uuid.uuid4()}/{output_filename}"

        # Upload translated file to S3
        s3_client.upload_file(tmp_output, BUCKET_NAME, output_s3_key)
        logger.info(f"Uploaded translated file to S3: {output_s3_key}")

        # Clean up temp files
        os.remove(tmp_input)
        os.remove(tmp_output)

        # Generate presigned URL for download
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": BUCKET_NAME, "Key": output_s3_key},
            ExpiresIn=3600,  # 1 hour
        )

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
            "body": json.dumps(
                {
                    "outputS3Key": output_s3_key,
                    "downloadUrl": presigned_url,
                    "stats": stats,
                }
            ),
        }

    except Exception as e:
        logger.exception("Error processing Excel translation")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
