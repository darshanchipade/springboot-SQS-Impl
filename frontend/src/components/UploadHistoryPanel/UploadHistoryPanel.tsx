import React from 'react';

const SearchIcon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
    </svg>
);

const FilterIcon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 mr-2" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M3 4h18M6 8h12M9 12h6" />
    </svg>
);

const UploadHistoryPanel = () => {
  return (
    <div className="bg-white p-6 rounded-lg shadow-md mt-8">
      <h2 className="text-xl font-semibold mb-4">Upload History</h2>
      <div className="flex space-x-4">
        <div className="relative flex-grow">
            <input type="text" placeholder="Search" className="w-full pl-10 pr-4 py-2 border rounded-lg" />
            <div className="absolute left-3 top-1/2 transform -translate-y-1/2">
                <SearchIcon />
            </div>
        </div>
        <button className="flex items-center px-4 py-2 border rounded-lg text-gray-600 hover:bg-gray-100">
            <FilterIcon />
            Filter
        </button>
      </div>
       <div className="text-center py-12 text-gray-400">
            Upload history will be displayed here.
       </div>
    </div>
  );
};

export default UploadHistoryPanel;