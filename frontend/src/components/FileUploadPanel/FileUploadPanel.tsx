"use client";

import React, { useState, useCallback } from 'react';
import { useDropzone } from 'react-dropzone';

const S3Icon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 mr-2 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8c1.657 0 3-1.343 3-3s-1.343-3-3-3-3 1.343-3 3 1.343 3 3 3zm0 2c-2.21 0-4 1.79-4 4v1h8v-1c0-2.21-1.79-4-4-4zm6 7H6v1a1 1 0 001 1h10a1 1 0 001-1v-1z" />
    </svg>
);


const UploadIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 mr-2 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
  </svg>
);

const ApiIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6 mr-2 text-gray-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M10 20l4-16m4 4l-4 4-4-4 4-4" />
  </svg>
);

const FileIcon = ({ fileName }) => {
    const extension = fileName.split('.').pop()?.toLowerCase();
    let bgColor = 'bg-gray-500';
    let textColor = 'text-white';
    let extLabel = extension?.toUpperCase();

    switch (extension) {
        case 'json':
            bgColor = 'bg-purple-200';
            textColor = 'text-purple-700';
            break;
        case 'pdf':
            bgColor = 'bg-red-200';
            textColor = 'text-red-700';
            break;
        case 'xls':
        case 'xlsx':
            bgColor = 'bg-green-200';
            textColor = 'text-green-700';
            extLabel = "XLS";
            break;
        case 'doc':
        case 'docx':
            bgColor = 'bg-blue-200';
            textColor = 'text-blue-700';
            extLabel = "DOC";
            break;
    }

    return (
        <div className={`w-12 h-12 rounded-lg flex-shrink-0 flex items-center justify-center ${bgColor} ${textColor} font-bold text-sm`}>
            {extLabel}
        </div>
    );
};


const FileUploadPanel = ({ setUploadedFiles }) => {
  const [activeTab, setActiveTab] = useState('local');
  const [files, setFiles] = useState([]);

  const onDrop = useCallback((acceptedFiles) => {
    const newFiles = acceptedFiles.map(file => ({
        file,
        name: file.name,
        id: Math.random().toString(36).substring(2, 9)
    }));
    setFiles(prevFiles => [...prevFiles, ...newFiles]);
    setUploadedFiles(prevFiles => [...prevFiles, ...newFiles]);
  }, [setUploadedFiles]);

  const { getRootProps, getInputProps, isDragActive } = useDropzone({ onDrop });

  const removeFile = (fileId) => {
    setFiles(prevFiles => prevFiles.filter(f => f.id !== fileId));
    setUploadedFiles(prevFiles => prevFiles.filter(f => f.id !== fileId));
  };


  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
      <h2 className="text-xl font-semibold mb-4">Upload Files</h2>
      <div className="flex border-b mb-4">
        <button onClick={() => setActiveTab('s3')} className={`py-2 px-4 flex items-center ${activeTab === 's3' ? 'border-b-2 border-blue-500 text-blue-500' : 'text-gray-500'}`}>
            <S3Icon /> Amazon S3 / Cloud
        </button>
        <button onClick={() => setActiveTab('local')} className={`py-2 px-4 flex items-center ${activeTab === 'local' ? 'border-b-2 border-blue-500 text-blue-500' : 'text-gray-500'}`}>
            <UploadIcon /> Local Upload
        </button>
        <button onClick={() => setActiveTab('api')} className={`py-2 px-4 flex items-center ${activeTab === 'api' ? 'border-b-2 border-blue-500 text-blue-500' : 'text-gray-500'}`}>
            <ApiIcon /> API Endpoint
        </button>
      </div>

      {activeTab === 'local' && (
        <div>
            <div {...getRootProps()} className={`border-2 border-dashed rounded-lg p-12 text-center cursor-pointer ${isDragActive ? 'border-blue-500 bg-blue-50' : 'border-gray-300'}`}>
                <input {...getInputProps()} />
                <p className="text-gray-500">Drag and drop or <span className="text-blue-500 font-semibold">Browse</span></p>
                <p className="text-sm text-gray-400 mt-1">PDF, DOCX, XLS, or JSON (max. 50 MB)</p>
            </div>
            <div className="mt-4 space-y-2">
                {files.map((file) => (
                    <div key={file.id} className="flex items-center justify-between p-2 border rounded-lg">
                       <div className="flex items-center">
                            <FileIcon fileName={file.name} />
                            <span className="ml-4 font-medium">{file.name}</span>
                       </div>
                        <button onClick={() => removeFile(file.id)} className="text-gray-500 hover:text-red-500">
                            <svg xmlns="http://www.w3.org/2000/svg" className="h-6 w-6" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                            </svg>
                        </button>
                    </div>
                ))}
            </div>
        </div>
      )}
       {activeTab !== 'local' && (
            <div className="text-center py-12 text-gray-500">
                This is a placeholder for {activeTab === 's3' ? 'Amazon S3 / Cloud' : 'API Endpoint'} functionality.
            </div>
        )}
    </div>
  );
};

export default FileUploadPanel;