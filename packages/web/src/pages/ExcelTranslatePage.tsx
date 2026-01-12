import React, { useState, useCallback, useRef, useEffect } from 'react';
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

type JobStatus = 'PENDING' | 'PROCESSING' | 'COMPLETED' | 'FAILED';

type ProgressInfo = {
  current_sheet?: string;
  sheets_processed?: number;
  total_sheets?: number;
  translated_cells?: number;
  total_translatable?: number;
  percent?: number;
  batch_progress?: string;
};

type JobStatusResponse = {
  jobId: string;
  status: JobStatus;
  createdAt?: string;
  startedAt?: string;
  completedAt?: string;
  failedAt?: string;
  downloadUrl?: string;
  outputS3Key?: string;
  stats?: {
    total_cells: number;
    translated_cells: number;
    sheets_processed: number;
  };
  progress?: ProgressInfo;
  error?: string;
};

type StartJobResponse = {
  jobId: string;
  status: string;
  message: string;
};

const POLLING_INTERVAL = 3000; // 3 seconds

const ExcelTranslatePage: React.FC = () => {
  const { t } = useTranslation();
  const { getSignedUrl, uploadFile } = useFileApi();
  const http = useHttp();

  const [file, setFile] = useState<File | null>(null);
  const [sourceLanguage, setSourceLanguage] = useState('Japanese');
  const [targetLanguage, setTargetLanguage] = useState('English');
  const [isUploading, setIsUploading] = useState(false);
  const [jobId, setJobId] = useState<string | null>(null);
  const [jobStatus, setJobStatus] = useState<JobStatus | null>(null);
  const [progress, setProgress] = useState<ProgressInfo | null>(null);
  const [result, setResult] = useState<JobStatusResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  const fileInputRef = useRef<HTMLInputElement>(null);
  const pollingRef = useRef<NodeJS.Timeout | null>(null);

  // Cleanup polling on unmount
  useEffect(() => {
    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
      }
    };
  }, []);

  const pollJobStatus = useCallback(
    async (id: string) => {
      try {
        // Use http.api.get directly (not http.get which uses SWR hook)
        const response = await http.api.get<JobStatusResponse>(
          `excel/translate/${id}`
        );
        const data = response.data;

        if (!data) {
          return;
        }

        setJobStatus(data.status);

        // Update progress information
        if (data.progress) {
          setProgress(data.progress);
        }

        if (data.status === 'COMPLETED') {
          setResult(data);
          if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
          }
        } else if (data.status === 'FAILED') {
          setError(data.error || t('excelTranslate.error.translationFailed'));
          if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
          }
        }
      } catch (err) {
        console.error('Error polling job status:', err);
        // Don't stop polling on transient errors
      }
    },
    [http.api, t]
  );

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
        setJobId(null);
        setJobStatus(null);
        setProgress(null);
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
    setJobId(null);
    setJobStatus(null);
    setProgress(null);

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
      const baseUrl = extractBaseURL(signedUrl);
      const urlObj = new URL(baseUrl);
      const s3Key = urlObj.pathname.slice(1);

      // Step 3: Start translation job (async)
      setJobStatus('PENDING');
      const response = await http.post<StartJobResponse>('excel/translate', {
        s3Key,
        sourceLanguage,
        targetLanguage,
      });

      const newJobId = response.data.jobId;
      setJobId(newJobId);

      // Step 4: Start polling for job status
      pollingRef.current = setInterval(() => {
        pollJobStatus(newJobId);
      }, POLLING_INTERVAL);

      // Also poll immediately
      pollJobStatus(newJobId);
    } catch (err) {
      setIsUploading(false);
      setJobStatus(null);
      setError(
        err instanceof Error
          ? err.message
          : t('excelTranslate.error.translationFailed')
      );
    }
  }, [
    file,
    sourceLanguage,
    targetLanguage,
    getSignedUrl,
    uploadFile,
    http,
    t,
    pollJobStatus,
  ]);

  const handleDownload = useCallback(() => {
    if (result?.downloadUrl) {
      window.open(result.downloadUrl, '_blank');
    }
  }, [result]);

  const handleReset = useCallback(() => {
    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }
    setFile(null);
    setResult(null);
    setError(null);
    setJobId(null);
    setJobStatus(null);
    setProgress(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  }, []);

  const isProcessing =
    isUploading ||
    (jobStatus !== null && !['COMPLETED', 'FAILED'].includes(jobStatus));

  const getStatusMessage = () => {
    if (isUploading) return t('excelTranslate.uploading');
    switch (jobStatus) {
      case 'PENDING':
        return t('excelTranslate.status.pending');
      case 'PROCESSING':
        return t('excelTranslate.status.processing');
      default:
        return t('excelTranslate.translateButton');
    }
  };

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

          {/* Processing Status */}
          {isProcessing && jobId && (
            <div className="mb-4 rounded-lg border border-blue-200 bg-blue-50 p-4">
              <div className="flex items-center gap-2">
                <PiSpinnerGap
                  className="animate-spin text-blue-600"
                  size={20}
                />
                <span className="text-blue-800">{getStatusMessage()}</span>
              </div>

              {/* Progress Bar */}
              {progress && progress.percent !== undefined && (
                <div className="mt-4">
                  <div className="mb-2 flex justify-between text-sm text-blue-700">
                    <span>
                      {progress.current_sheet
                        ? t('excelTranslate.progress.currentSheet', {
                            sheet: progress.current_sheet,
                          })
                        : t('excelTranslate.progress.processing')}
                    </span>
                    {/* eslint-disable-next-line @shopify/jsx-no-hardcoded-content */}
                    <span>{progress.percent}%</span>
                  </div>
                  <div className="h-3 w-full overflow-hidden rounded-full bg-blue-200">
                    <div
                      className="h-full rounded-full bg-blue-600 transition-all duration-300"
                      style={{ width: `${progress.percent}%` }}
                    />
                  </div>
                  <div className="mt-2 flex flex-wrap gap-4 text-xs text-blue-600">
                    {progress.translated_cells !== undefined &&
                      progress.total_translatable !== undefined && (
                        <span>
                          {t('excelTranslate.progress.cells', {
                            translated: progress.translated_cells,
                            total: progress.total_translatable,
                          })}
                        </span>
                      )}
                    {progress.sheets_processed !== undefined &&
                      progress.total_sheets !== undefined && (
                        <span>
                          {t('excelTranslate.progress.sheets', {
                            processed: progress.sheets_processed,
                            total: progress.total_sheets,
                          })}
                        </span>
                      )}
                  </div>
                </div>
              )}

              <p className="mt-3 text-sm text-blue-600">
                {t('excelTranslate.status.asyncNote')}
              </p>
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
              {getStatusMessage()}
            </Button>
            <Button outlined onClick={handleReset} disabled={isProcessing}>
              {t('excelTranslate.resetButton')}
            </Button>
          </div>

          {/* Result */}
          {result && result.status === 'COMPLETED' && (
            <div className="mt-6 rounded-lg border border-green-200 bg-green-50 p-4">
              <h3 className="mb-2 font-medium text-green-800">
                {t('excelTranslate.success.title')}
              </h3>
              <div className="mb-4 text-sm text-green-700">
                {result.stats && (
                  <>
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
                  </>
                )}
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
