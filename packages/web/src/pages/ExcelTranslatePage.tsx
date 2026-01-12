import React, { useState, useCallback, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import Card from '../components/Card';
import Button from '../components/Button';
import Select from '../components/Select';
import Alert from '../components/Alert';
import { PiFileXls, PiDownload, PiSpinnerGap } from 'react-icons/pi';
import useFileApi from '../hooks/useFileApi';
import useHttp from '../hooks/useHttp';
import { extractBaseURL } from '../hooks/useFiles';

/* eslint-disable i18nhelper/no-jp-string */
const LANGUAGES = [
  { value: 'Japanese', label: '日本語' },
  { value: 'English', label: 'English' },
  { value: 'Chinese', label: '中文' },
  { value: 'Korean', label: '한국어' },
  { value: 'Spanish', label: 'Español' },
  { value: 'French', label: 'Français' },
  { value: 'German', label: 'Deutsch' },
  { value: 'Portuguese', label: 'Português' },
];
/* eslint-enable i18nhelper/no-jp-string */

type TranslationResult = {
  outputS3Key: string;
  downloadUrl: string;
  stats: {
    total_cells: number;
    translated_cells: number;
    sheets_processed: number;
  };
};

const ExcelTranslatePage: React.FC = () => {
  const { t } = useTranslation();
  const { getSignedUrl, uploadFile } = useFileApi();
  const http = useHttp();

  const [file, setFile] = useState<File | null>(null);
  const [sourceLanguage, setSourceLanguage] = useState('Japanese');
  const [targetLanguage, setTargetLanguage] = useState('English');
  const [isUploading, setIsUploading] = useState(false);
  const [isTranslating, setIsTranslating] = useState(false);
  const [result, setResult] = useState<TranslationResult | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const files = e.target.files;
      if (files && files[0]) {
        const selectedFile = files[0];
        // Validate file type
        if (
          !selectedFile.name.endsWith('.xlsx') &&
          !selectedFile.name.endsWith('.xls')
        ) {
          setError(t('excelTranslate.error.invalidFileType'));
          return;
        }
        setFile(selectedFile);
        setError(null);
        setResult(null);
      }
    },
    [t]
  );

  const handleTranslate = useCallback(async () => {
    if (!file) {
      setError(t('excelTranslate.error.noFile'));
      return;
    }

    if (sourceLanguage === targetLanguage) {
      setError(t('excelTranslate.error.sameLanguage'));
      return;
    }

    setError(null);
    setResult(null);

    try {
      // Step 1: Get signed URL and upload file
      setIsUploading(true);
      const signedUrl = (
        await getSignedUrl({
          filename: file.name,
          mediaFormat: 'xlsx',
        })
      ).data;

      await uploadFile(signedUrl, { file });
      setIsUploading(false);

      // Step 2: Extract S3 key from signed URL
      // URL format: https://bucket.s3.region.amazonaws.com/uuid/filename.xlsx?...
      const baseUrl = extractBaseURL(signedUrl);
      const urlObj = new URL(baseUrl);
      // Remove leading slash from pathname to get S3 key
      const s3Key = urlObj.pathname.slice(1);

      // Step 3: Call translation API
      setIsTranslating(true);
      const response = await http.post<TranslationResult>('excel/translate', {
        s3Key,
        sourceLanguage,
        targetLanguage,
      });

      setResult(response.data);
      setIsTranslating(false);
    } catch (err) {
      setIsUploading(false);
      setIsTranslating(false);
      setError(
        err instanceof Error
          ? err.message
          : t('excelTranslate.error.translationFailed')
      );
    }
  }, [file, sourceLanguage, targetLanguage, getSignedUrl, uploadFile, http, t]);

  const handleDownload = useCallback(() => {
    if (result?.downloadUrl) {
      window.open(result.downloadUrl, '_blank');
    }
  }, [result]);

  const handleReset = useCallback(() => {
    setFile(null);
    setResult(null);
    setError(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  }, []);

  const isProcessing = isUploading || isTranslating;

  return (
    <div className="grid grid-cols-12">
      <div className="invisible col-span-12 my-0 flex h-0 items-center justify-center lg:visible lg:my-5 lg:h-min">
        <span className="text-xl font-bold">{t('excelTranslate.title')}</span>
      </div>

      <div className="col-span-12 col-start-1 mx-2 lg:col-span-10 lg:col-start-2 xl:col-span-8 xl:col-start-3">
        <Card>
          <div className="mb-4">
            <p className="text-gray-600">{t('excelTranslate.description')}</p>
          </div>

          {/* File Input */}
          <div className="mb-6">
            <label className="mb-2 block font-medium">
              {t('excelTranslate.fileLabel')}
            </label>
            <div className="flex items-center gap-4">
              <input
                ref={fileInputRef}
                type="file"
                accept=".xlsx,.xls"
                onChange={handleFileChange}
                disabled={isProcessing}
                className="border-aws-font-color/20 block w-full cursor-pointer rounded-lg border bg-white text-sm file:mr-4 file:border-0 file:bg-gray-100 file:px-4 file:py-2 file:text-sm"
              />
            </div>
            {file && (
              <div className="mt-2 flex items-center gap-2 text-sm text-gray-600">
                <PiFileXls className="text-green-600" size={20} />
                <span>{file.name}</span>
                {/* eslint-disable-next-line @shopify/jsx-no-hardcoded-content */}
                <span className="text-gray-400">
                  ({(file.size / 1024).toFixed(1)} KB)
                </span>
              </div>
            )}
          </div>

          {/* Language Selection */}
          <div className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-2">
            <div>
              <label className="mb-2 block font-medium">
                {t('excelTranslate.sourceLanguage')}
              </label>
              <Select
                value={sourceLanguage}
                onChange={setSourceLanguage}
                options={LANGUAGES.map((lang) => ({
                  value: lang.value,
                  label: lang.label,
                }))}
              />
            </div>
            <div>
              <label className="mb-2 block font-medium">
                {t('excelTranslate.targetLanguage')}
              </label>
              <Select
                value={targetLanguage}
                onChange={setTargetLanguage}
                options={LANGUAGES.map((lang) => ({
                  value: lang.value,
                  label: lang.label,
                }))}
              />
            </div>
          </div>

          {/* Error Alert */}
          {error && (
            <div className="mb-4">
              <Alert severity="error" title={t('excelTranslate.error.title')}>
                {error}
              </Alert>
            </div>
          )}

          {/* Action Buttons */}
          <div className="flex gap-4">
            <Button
              onClick={handleTranslate}
              disabled={!file || isProcessing}
              className="flex items-center gap-2">
              {isProcessing && (
                <PiSpinnerGap className="animate-spin" size={20} />
              )}
              {isUploading
                ? t('excelTranslate.uploading')
                : isTranslating
                  ? t('excelTranslate.translating')
                  : t('excelTranslate.translateButton')}
            </Button>
            <Button outlined onClick={handleReset} disabled={isProcessing}>
              {t('excelTranslate.resetButton')}
            </Button>
          </div>

          {/* Result */}
          {result && (
            <div className="mt-6 rounded-lg border border-green-200 bg-green-50 p-4">
              <h3 className="mb-2 font-medium text-green-800">
                {t('excelTranslate.success.title')}
              </h3>
              <div className="mb-4 text-sm text-green-700">
                <p>
                  {t('excelTranslate.success.sheetsProcessed', {
                    count: result.stats.sheets_processed,
                  })}
                </p>
                <p>
                  {t('excelTranslate.success.cellsTranslated', {
                    translated: result.stats.translated_cells,
                    total: result.stats.total_cells,
                  })}
                </p>
              </div>
              <Button
                onClick={handleDownload}
                className="flex items-center gap-2">
                <PiDownload size={20} />
                {t('excelTranslate.downloadButton')}
              </Button>
            </div>
          )}
        </Card>
      </div>
    </div>
  );
};

export default ExcelTranslatePage;
