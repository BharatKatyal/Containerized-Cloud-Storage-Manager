import React, { useState, useEffect } from 'react';
import { Upload, Trash2, Edit, Save } from 'lucide-react';

const FileManager = () => {
  const [files, setFiles] = useState([]);
  const [editingId, setEditingId] = useState(null);
  const [editName, setEditName] = useState('');
  const API_URL = 'http://localhost:5000';

  useEffect(() => {
    fetchFiles();
  }, []);

  const fetchFiles = async () => {
    try {
      const response = await fetch(`${API_URL}/files`);
      const data = await response.json();
      setFiles(data);
    } catch (error) {
      console.error('Error fetching files:', error);
    }
  };

  const handleFileUpload = async (event) => {
    const file = event.target.files[0];
    if (!file) return;

    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await fetch(`${API_URL}/upload`, {
        method: 'POST',
        body: formData,
      });
      if (response.ok) {
        fetchFiles();
      }
    } catch (error) {
      console.error('Error uploading file:', error);
    }
  };

  const handleDelete = async (fileId) => {
    try {
      const response = await fetch(`${API_URL}/files/${fileId}`, {
        method: 'DELETE',
      });
      if (response.ok) {
        fetchFiles();
      }
    } catch (error) {
      console.error('Error deleting file:', error);
    }
  };

  const startEditing = (file) => {
    setEditingId(file.id);
    setEditName(file.name);
  };

  const handleUpdate = async (fileId) => {
    try {
      const response = await fetch(`${API_URL}/files/${fileId}`, {
        method: 'PUT',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ name: editName }),
      });
      if (response.ok) {
        setEditingId(null);
        fetchFiles();
      }
    } catch (error) {
      console.error('Error updating file:', error);
    }
  };

  return (
    <div className="container mx-auto p-6">
      <div className="mb-8">
        <label className="flex items-center justify-center w-full h-32 border-2 border-dashed border-gray-300 rounded-lg cursor-pointer hover:bg-gray-50">
          <div className="space-y-1 text-center">
            <Upload className="mx-auto h-12 w-12 text-gray-400" />
            <div className="text-sm text-gray-600">
              <span className="font-medium text-blue-600">Click to upload</span> or drag and drop
            </div>
          </div>
          <input type="file" className="hidden" onChange={handleFileUpload} />
        </label>
      </div>

      <div className="bg-white rounded-lg shadow">
        <div className="flex flex-col">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Name
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Size
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Last Modified
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-medium text-gray-500 uppercase tracking-wider">
                    Actions
                  </th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {files.map((file) => (
                  <tr key={file.id}>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {editingId === file.id ? (
                        <input
                          type="text"
                          value={editName}
                          onChange={(e) => setEditName(e.target.value)}
                          className="border rounded px-2 py-1"
                        />
                      ) : (
                        file.name
                      )}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {Math.round(file.size / 1024)} KB
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap">
                      {new Date(file.last_modified).toLocaleDateString()}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right text-sm font-medium space-x-2">
                      {editingId === file.id ? (
                        <button
                          onClick={() => handleUpdate(file.id)}
                          className="text-green-600 hover:text-green-900"
                        >
                          <Save className="h-5 w-5" />
                        </button>
                      ) : (
                        <button
                          onClick={() => startEditing(file)}
                          className="text-blue-600 hover:text-blue-900"
                        >
                          <Edit className="h-5 w-5" />
                        </button>
                      )}
                      <button
                        onClick={() => handleDelete(file.id)}
                        className="text-red-600 hover:text-red-900"
                      >
                        <Trash2 className="h-5 w-5" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
  );
};

export default FileManager;
