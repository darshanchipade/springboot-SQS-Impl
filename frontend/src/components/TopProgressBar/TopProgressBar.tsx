import React from 'react';

const CheckCircleIcon = () => (
  <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-green-500" viewBox="0 0 20 20" fill="currentColor">
    <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
  </svg>
);

const UploadIcon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-8 w-8 text-white" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-8l-4-4m0 0L8 8m4-4v12" />
    </svg>
);


const Step = ({ icon, title, status, isLast }) => {
  const isActive = status === 'active';
  const isCompleted = status === 'completed';

  return (
    <div className="flex items-center">
      <div className="flex flex-col items-center">
        <div className={`w-16 h-16 rounded-full flex items-center justify-center ${isActive ? 'bg-blue-500' : 'bg-gray-300'}`}>
          {isActive ? <UploadIcon /> : icon}
        </div>
        <div className="mt-2 text-center">
          <p className={`font-semibold ${isActive ? 'text-blue-500' : 'text-gray-500'}`}>{title}</p>
          {isCompleted && <CheckCircleIcon />}
        </div>
      </div>
      {!isLast && <div className="flex-auto border-t-2 border-gray-300 mx-4"></div>}
    </div>
  );
};

const TopProgressBar = () => {
  const steps = [
    { title: 'Ingestion', icon: <UploadIcon />, status: 'active' },
    { title: 'Extraction', icon: <div className="w-8 h-8 bg-gray-400 rounded-full" />, status: 'inactive' },
    { title: 'Cleansing', icon: <div className="w-8 h-8 bg-gray-400 rounded-full" />, status: 'inactive' },
    { title: 'Data Enrichment', icon: <div className="w-8 h-8 bg-gray-400 rounded-full" />, status: 'inactive' },
    { title: 'Content QA', icon: <div className="w-8 h-8 bg-gray-400 rounded-full" />, status: 'inactive' },
  ];

  return (
    <div className="w-full py-8">
        <div className="flex items-center justify-center">
            {steps.map((step, index) => (
                <Step
                key={index}
                icon={step.icon}
                title={step.title}
                status={step.status}
                isLast={index === steps.length - 1}
                />
            ))}
        </div>
    </div>
  );
};

export default TopProgressBar;