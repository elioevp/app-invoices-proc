
import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import authService from '../services/authService';

const Login = ({ onLoginSuccess }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [isRegister, setIsRegister] = useState(false);
  const navigate = useNavigate(); // Initialize useNavigate

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      if (isRegister) {
        await authService.register({ username, password });
        alert('Registration successful! Please log in.');
        setIsRegister(false);
      } else {
        const { data } = await authService.login({ username, password });
        localStorage.setItem('token', data.token);
        onLoginSuccess(); // Notify App.js of successful login
        navigate('/dashboard'); // Use navigate for redirection
      }
    } catch (error) {
      console.error('Registration/Login Error:', error);
      alert('Error! Check console for details.');
    }
  };

  return (
    <div style={{ textAlign: 'center', padding: '20px' }}>
      <h1>facturas-proc</h1>
      <p style={{ fontSize: '1.2em', color: '#666' }}>Procesa tus facturas con IA y guarda los datos a la nube, mantén el control de tus gastos</p>
      <h2>{isRegister ? 'Register' : 'Login'}</h2>
      <form onSubmit={handleSubmit}>
        <input type="text" placeholder="Username" value={username} onChange={(e) => setUsername(e.target.value)} style={{ padding: '10px', fontSize: '1.3em', margin: '5px 0' }} />
        <input type="password" placeholder="Password" value={password} onChange={(e) => setPassword(e.target.value)} style={{ padding: '10px', fontSize: '1.3em', margin: '5px 0' }} />
        <button type="submit" style={{ padding: '10px 20px', fontSize: '1.3em', margin: '5px 0' }}>{isRegister ? 'Register' : 'Login'}</button>
      </form>
      <button onClick={() => setIsRegister(!isRegister)} style={{ padding: '10px 20px', fontSize: '1.3em', margin: '5px 0' }}>
        {isRegister ? 'Switch to Login' : 'Switch to Register'}
      </button>
    </div>
  );
};

export default Login;
