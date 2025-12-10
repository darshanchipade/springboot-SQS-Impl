"use client";

import React, { useState } from 'react';
import TopProgressBar from '@/components/TopProgressBar/TopProgressBar';
import FileUploadPanel from '@/components/FileUploadPanel/FileUploadPanel';
import ItemSelectionPanel from '@/components/ItemSelectionPanel/ItemSelectionPanel';
import UploadHistoryPanel from '@/components/UploadHistoryPanel/UploadHistoryPanel';

export default function Home() {
  const [uploadedFiles, setUploadedFiles] = useState([]);

  const handleExtract = async () => {
    if (uploadedFiles.length === 0) {
      alert("Please upload a file first.");
      return;
    }

    const formData = new FormData();
    // For now, we only support single file upload as per backend logic
    formData.append('file', uploadedFiles[0].file);

    try {
      const apiUrl = process.env.NEXT_PUBLIC_API_URL || '';
      const response = await fetch(`${apiUrl}/api/extract-cleanse-enrich-and-store`, {
        method: 'POST',
        body: formData,
      });

      if (response.ok) {
        const result = await response.text();
        alert(`File uploaded successfully: ${result}`);
      } else {
        const errorText = await response.text();
        alert(`Upload failed: ${errorText}`);
      }
    } catch (error) {
      console.error('Error uploading file:', error);
      alert('An error occurred while uploading the file.');
    }
  };


  return (
    <div className="bg-gray-50 min-h-screen">
      <header className="bg-white shadow-sm">
        <div className="max-w-7xl mx-auto py-4 px-4 sm:px-6 lg:px-8">
            <img src="https://upload.wikimedia.org/wikipedia/commons/f/fa/Apple_logo_black.svg" alt="Logo" className="h-8"/>
        </div>
      </header>

      <main className="max-w-7xl mx-auto py-6 sm:px-6 lg:px-8">
        <TopProgressBar />

        <div className="mt-8">
            <h1 className="text-3xl font-bold text-gray-900">Ingestion</h1>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mt-6">
          <div>
            <FileUploadPanel setUploadedFiles={setUploadedFiles} />
            <UploadHistoryPanel />
          </div>
          <div>
            <ItemSelectionPanel onExtract={handleExtract} uploadedFiles={uploadedFiles}/>
          </div>
        </div>

      </main>
    </div>
  );
}