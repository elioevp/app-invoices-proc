import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import directoryService from '../services/directoryService';
import { jwtDecode } from 'jwt-decode';

const Dashboard = () => {
  const [directories, setDirectories] = useState([]);
  const [newDirectory, setNewDirectory] = useState('');
  const [selectedDirectory, setSelectedDirectory] = useState(null);
  const [filesInSelectedDirectory, setFilesInSelectedDirectory] = useState([]);
  const [selectedFile, setSelectedFile] = useState(null);
  const [username, setUsername] = useState('');
  const navigate = useNavigate();

  const handleLogout = useCallback(() => {
    localStorage.removeItem('token');
    navigate('/');
  }, [navigate]);

  const fetchDirectories = useCallback(async () => {
    try {
      const { data } = await directoryService.getDirectories();
      setDirectories(data);
    } catch (error) {
      console.error('Error fetching directories:', error);
    }
  }, []);

  useEffect(() => {
    const token = localStorage.getItem('token');
    if (token) {
      try {
        const decodedToken = jwtDecode(token);
        setUsername(decodedToken.username);
      } catch (error) {
        console.error('Error decoding token:', error);
        handleLogout(); // Log out if token is invalid
      }
    }
    fetchDirectories();
  }, [fetchDirectories, handleLogout]);

  const handleCreateDirectory = async () => {
    try {
      await directoryService.createDirectory({ directoryName: newDirectory });
      setNewDirectory('');
      fetchDirectories();
    } catch (error) {
      console.error('Error creating directory:', error);
    }
  };

  const handleDirectoryClick = async (directoryName) => {
    setSelectedDirectory(directoryName);
    setSelectedFile(null); // Clear selected file when changing directory
    try {
      const { data } = await directoryService.getFilesInDirectory(directoryName);
      setFilesInSelectedDirectory(data);
    } catch (error) {
      console.error('Error fetching files in directory:', error);
      setFilesInSelectedDirectory([]);
    }
  };

  const handleFileClick = (file) => {
    setSelectedFile(file);
  };

  const handleFileUpload = async (directoryName) => {
    if (!selectedFile) {
      alert('Please select a file to upload.');
      return;
    }
    const formData = new FormData();
    formData.append('file', selectedFile);
    try {
      await directoryService.uploadFile(directoryName, formData);
      alert('File uploaded!');
      // Refresh files in the current directory after upload
      if (selectedDirectory) {
        handleDirectoryClick(selectedDirectory);
      }
    } catch (error) {
      console.error('Error uploading file:', error);
      alert('File upload failed!');
    }
  };

  const handleDeleteFile = async (directoryName, fileName, event) => {
    event.stopPropagation(); // Prevent triggering handleFileClick
    if (window.confirm(`Are you sure you want to delete ${fileName}?`)) {
      try {
        await directoryService.deleteFile(directoryName, fileName);
        alert('File deleted successfully!');
        // Refresh files in the current directory after deletion
        if (selectedDirectory) {
          handleDirectoryClick(selectedDirectory);
        }
        setSelectedFile(null); // Clear selected file if it was the one deleted
      } catch (error) {
        console.error('Error deleting file:', error);
        alert('File deletion failed!');
      }
    }
  };

  return (
    <div style={{ display: 'flex', minHeight: '100vh', flexDirection: 'column' }}>
      <h1 style={{ textAlign: 'center', padding: '10px' }}>validator-v</h1>
      <div style={{ padding: '10px', borderBottom: '1px solid #ccc', display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
        <h2>Dashboard</h2>
        {username && <p style={{ color: 'red', fontSize: '1.2em', fontWeight: 'bold' }}>Welcome, {username}!</p>}
        <button onClick={handleLogout} style={{ padding: '10px 20px', fontSize: '1.3em' }}>Logout</button>
      </div>
      <div style={{ display: 'flex', flex: 1 }}>
        {/* Left Panel: Directories */}
        <div style={{ width: '20%', borderRight: '1px solid #ccc', padding: '10px', overflowY: 'auto' }}>
          <h3>Directories</h3>
          <input
            type="text"
            value={newDirectory}
            onChange={(e) => setNewDirectory(e.target.value)}
            placeholder="New directory name"
          />
          <button onClick={handleCreateDirectory}>Create</button>
          <hr />
          {directories.map((dir) => (
            <div
              key={dir}
              onClick={() => handleDirectoryClick(dir)}
              style={{
                cursor: 'pointer',
                padding: '5px',
                backgroundColor: selectedDirectory === dir ? '#f0f0f0' : 'transparent',
              }}
            >
              {dir}
            </div>
          ))}
        </div>

        {/* Middle Panel: Files in Selected Directory */}
        <div style={{ flex: '0 0 350px', borderRight: '1px solid #ccc', padding: '10px', overflowY: 'auto' }}>
          <h3>Files {selectedDirectory && `in ${selectedDirectory}`}</h3>
          {selectedDirectory && (
            <div style={{ marginBottom: '10px' }}>
              <input type="file" onChange={(e) => setSelectedFile(e.target.files[0])} />
              <button onClick={() => handleFileUpload(selectedDirectory)}>Upload to {selectedDirectory}</button>
            </div>
          )}
          <hr />
          {filesInSelectedDirectory.length > 0 ? (
            filesInSelectedDirectory.map((file) => (
              <div
                key={file.name}
                style={{
                  display: 'flex',
                  justifyContent: 'space-between',
                  alignItems: 'center',
                  padding: '5px',
                  backgroundColor: selectedFile === file ? '#f0f0f0' : 'transparent',
                }}
              >
                <span
                  onClick={() => handleFileClick(file)}
                  style={{ cursor: 'pointer', flexGrow: 1 }}
                >
                  {file.name}
                </span>
                <button
                  onClick={(e) => handleDeleteFile(selectedDirectory, file.name, e)}
                  style={{ marginLeft: '10px', cursor: 'pointer' }}
                >
                  Delete
                </button>
              </div>
            ))
          ) : (
            <p>{selectedDirectory ? 'No files in this directory.' : 'Select a directory to view files.'}</p>
          )}
        </div>

        {/* Right Panel: Photo Viewer */}
        <div style={{ flex: 1, padding: '10px', display: 'flex', justifyContent: 'center', alignItems: 'center', overflow: 'hidden' }}>
          {selectedFile ? (
            <img src={selectedFile.url} alt={selectedFile.name} style={{ maxWidth: '100%', maxHeight: '100%', objectFit: 'contain' }} />
          ) : (
            <p>Click on a file to view its content.</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default Dashboard;

//Hasta el 196 el codigo
//fin del dashboard

