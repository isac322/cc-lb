import { Upload } from 'lucide-react';
import { useRef, useState } from 'react';
import { getAdminToken } from '../lib/auth';
import type { UploadResponse } from '../lib/types/v1';

interface WasmUploaderProps {
  onSuccess: (response: UploadResponse) => void;
}

export function WasmUploader({ onSuccess }: WasmUploaderProps) {
  const [file, setFile] = useState<File | null>(null);
  const [name, setName] = useState('');
  const [error, setError] = useState<string | null>(null);
  const [progress, setProgress] = useState<number | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const selected = e.target.files?.[0];
    if (!selected) return;

    setError(null);

    if (selected.size > 32 * 1024 * 1024) {
      setError('File exceeds 32 MiB limit');
      setFile(null);
      if (fileInputRef.current) fileInputRef.current.value = '';
      return;
    }

    const buffer = await selected.slice(0, 4).arrayBuffer();
    const view = new Uint8Array(buffer);
    if (
      view.length < 4 ||
      view[0] !== 0x00 ||
      view[1] !== 0x61 ||
      view[2] !== 0x73 ||
      view[3] !== 0x6d
    ) {
      setError('Invalid Wasm file (missing magic bytes)');
      setFile(null);
      if (fileInputRef.current) fileInputRef.current.value = '';
      return;
    }

    setFile(selected);
    if (!name) {
      setName(selected.name.replace(/\.wasm$/, ''));
    }
  };

  const handleUpload = () => {
    if (!file || !name) return;

    setError(null);
    setProgress(0);

    const formData = new FormData();
    formData.append('name', name);
    formData.append('original_filename', file.name);
    formData.append('bytes', file);

    const xhr = new XMLHttpRequest();
    xhr.open('POST', '/admin/v1/plugins/wasm');

    const token = getAdminToken();
    if (token) {
      xhr.setRequestHeader('Authorization', `Bearer ${token}`);
    }

    xhr.upload.onprogress = (e) => {
      if (e.lengthComputable) {
        setProgress(Math.round((e.loaded / e.total) * 100));
      }
    };

    xhr.onload = () => {
      setProgress(null);
      if (xhr.status >= 200 && xhr.status < 300) {
        try {
          const response = JSON.parse(xhr.responseText) as UploadResponse;
          setFile(null);
          setName('');
          if (fileInputRef.current) fileInputRef.current.value = '';
          onSuccess(response);
        } catch (_err) {
          setError('Failed to parse response');
        }
      } else {
        try {
          const body = JSON.parse(xhr.responseText);
          setError(
            body.reason ||
              body.error ||
              `Upload failed with status ${xhr.status}`,
          );
        } catch {
          setError(`Upload failed with status ${xhr.status}`);
        }
      }
    };

    xhr.onerror = () => {
      setProgress(null);
      setError('Network error occurred during upload');
    };

    xhr.send(formData);
  };

  return (
    <div className="p-4 border rounded-lg bg-white shadow-sm space-y-4">
      <h3 className="text-lg font-medium">Upload Wasm Plugin</h3>

      <div className="space-y-2">
        <label className="block text-sm font-medium text-gray-700">
          Wasm File
        </label>
        <input
          type="file"
          accept=".wasm"
          onChange={handleFileChange}
          ref={fileInputRef}
          className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded-md file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
        />
      </div>

      {file && (
        <div className="space-y-2">
          <label className="block text-sm font-medium text-gray-700">
            Plugin Name
          </label>
          <input
            type="text"
            value={name}
            onChange={(e) => setName(e.target.value)}
            className="block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500 sm:text-sm border p-2"
            placeholder="my-plugin"
          />
        </div>
      )}

      {error && <div className="text-sm text-red-600">{error}</div>}

      {progress !== null && (
        <div className="w-full bg-gray-200 rounded-full h-2.5">
          <div
            className="bg-blue-600 h-2.5 rounded-full transition-all duration-300"
            style={{ width: `${progress}%` }}
          ></div>
        </div>
      )}

      <button
        onClick={handleUpload}
        disabled={!file || !name || progress !== null}
        className="inline-flex items-center px-4 py-2 border border-transparent text-sm font-medium rounded-md shadow-sm text-white bg-blue-600 hover:bg-blue-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-blue-500 disabled:opacity-50 disabled:cursor-not-allowed"
      >
        <Upload className="w-4 h-4 mr-2" />
        {progress !== null ? `Uploading ${progress}%` : 'Upload'}
      </button>
    </div>
  );
}
