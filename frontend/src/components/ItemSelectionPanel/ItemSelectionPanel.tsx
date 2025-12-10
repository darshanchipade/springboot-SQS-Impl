"use client";

import React, { useState } from 'react';

const SearchIcon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
    </svg>
);

const ChevronDownIcon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
        <path fillRule="evenodd" d="M5.293 7.293a1 1 0 011.414 0L10 10.586l3.293-3.293a1 1 0 111.414 1.414l-4 4a1 1 0 01-1.414 0l-4-4a1 1 0 010-1.414z" clipRule="evenodd" />
    </svg>
);

const ChevronRightIcon = () => (
    <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5" viewBox="0 0 20 20" fill="currentColor">
        <path fillRule="evenodd" d="M7.293 14.707a1 1 0 010-1.414L10.586 10 7.293 6.707a1 1 0 011.414-1.414l4 4a1 1 0 010 1.414l-4 4a1 1 0 01-1.414 0z" clipRule="evenodd" />
    </svg>
);


const DUMMY_JSON_TREE = {
    "Content.JSON": {
      "product_information": {
        "product_name": {},
        "subtitle": {},
        "description": {},
        "specifications": {},
        "pricing": {},
        "images": {},
        "metadata": {}
      }
    }
  };

  const TreeView = ({ node, name, selectedItems, handleToggle }) => {
    const [isOpen, setIsOpen] = useState(true);
    const children = Object.keys(node);
    const isSelected = selectedItems[name];

    const toggleOpen = () => setIsOpen(!isOpen);

    const handleCheckboxChange = (e) => {
        handleToggle(name, e.target.checked);
    };

    return (
        <div className="ml-4">
            <div className="flex items-center">
                <input type="checkbox" className="form-checkbox h-5 w-5 text-blue-600 rounded" checked={isSelected} onChange={handleCheckboxChange} />
                <span className="ml-2 font-semibold">{name}</span>
                {children.length > 0 && (
                     <button onClick={toggleOpen} className="ml-auto">
                        {isOpen ? <ChevronDownIcon /> : <ChevronRightIcon />}
                    </button>
                )}
            </div>
            {isOpen && children.length > 0 && (
                <div className="border-l-2 pl-4 ml-2 mt-1">
                    {children.map(key => (
                        <TreeView key={key} node={node[key]} name={key} selectedItems={selectedItems} handleToggle={handleToggle} />
                    ))}
                </div>
            )}
        </div>
    );
};


const ItemSelectionPanel = ({ onExtract, uploadedFiles }) => {
    const [selectedItems, setSelectedItems] = useState({ "Content.JSON": true, "product_information": true, "product_name": true, "subtitle": true, "description": true });
    const [treeData, setTreeData] = useState(DUMMY_JSON_TREE);

    const handleToggle = (name, isSelected) => {
        setSelectedItems(prev => ({...prev, [name]: isSelected}));
    };

    const selectedCount = Object.values(selectedItems).filter(Boolean).length;
    const selectedKeys = Object.entries(selectedItems)
        .filter(([,isSelected]) => isSelected)
        .map(([key]) => key)
        .slice(0, 8);
    const remainingCount = selectedCount > 8 ? selectedCount - 8 : 0;


  return (
    <div className="bg-white p-6 rounded-lg shadow-md">
        <h2 className="text-xl font-semibold mb-4">Select Items</h2>
        <div className="relative mb-4">
            <input type="text" placeholder="Search..." className="w-full pl-10 pr-4 py-2 border rounded-lg" />
            <div className="absolute left-3 top-1/2 transform -translate-y-1/2">
                <SearchIcon />
            </div>
        </div>

        <div className="mb-4">
             {Object.keys(treeData).map(key => (
                <TreeView key={key} node={treeData[key]} name={key} selectedItems={selectedItems} handleToggle={handleToggle} />
            ))}
        </div>

        <div>
            <div className="flex justify-between items-center mb-2">
                <h3 className="font-semibold">Selected</h3>
                <span className="bg-blue-500 text-white text-sm font-bold px-3 py-1 rounded-full">{selectedCount} items</span>
            </div>
            <div className="flex flex-wrap gap-2">
                {selectedKeys.map(key => (
                     <span key={key} className="bg-gray-200 text-gray-700 px-2 py-1 rounded-md text-sm">{key}</span>
                ))}
                {remainingCount > 0 && (
                     <span className="bg-gray-200 text-gray-700 px-2 py-1 rounded-md text-sm">+{remainingCount} more</span>
                )}
            </div>
        </div>

      <button
        onClick={onExtract}
        disabled={uploadedFiles.length === 0}
        className="w-full bg-gray-800 text-white py-3 rounded-lg font-semibold mt-6 hover:bg-gray-900 disabled:bg-gray-400"
      >
        Extract Data
      </button>
    </div>
  );
};

export default ItemSelectionPanel;