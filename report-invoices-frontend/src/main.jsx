import React from 'react'
import ReactDOM from 'react-dom/client'
import { createBrowserRouter, RouterProvider } from 'react-router-dom';
import DashboardPage from './pages/DashboardPage.jsx';
import 'bootstrap/dist/css/bootstrap.min.css';
import './index.css'

// Simplified router setup
const router = createBrowserRouter([
  {
    path: "/",
    element: <DashboardPage />,
  },
]);

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <RouterProvider router={router} />
  </React.StrictMode>,
)