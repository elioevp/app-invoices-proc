import api from './api';

const API_URL = '/api/directories';

const getAuthHeaders = () => {
  const token = localStorage.getItem('token');
  return { headers: { Authorization: `Bearer ${token}` } };
};

const getDirectories = () => {
  return api.get(API_URL, getAuthHeaders());
};

const createDirectory = (data) => {
  return api.post(API_URL, data, getAuthHeaders());
};

const uploadFile = (directoryName, data) => {
  return api.post(`${API_URL}/upload/${directoryName}`, data, getAuthHeaders());
};

const getFilesInDirectory = (directoryName) => {
  return api.get(`${API_URL}/${directoryName}/files`, getAuthHeaders());
};

const deleteFile = (directoryName, fileName) => {
  return api.delete(`${API_URL}/${directoryName}/files/${fileName}`, getAuthHeaders());
};

const directoryService = { getDirectories, createDirectory, uploadFile, getFilesInDirectory, deleteFile };

export default directoryService;