import api from './api';

const API_URL = '/api/auth';

const register = (data) => {
  return api.post(`${API_URL}/register`, data);
};

const login = (data) => {
  return api.post(`${API_URL}/login`, data);
};

const authService = {
  register,
  login
};

export default authService;