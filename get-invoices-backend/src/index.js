
const express = require('express');
const cors = require('cors');
require('dotenv').config();

const app = express();

app.use(cors());
app.use(express.json());

// Rutas
const authRoutes = require('./routes/auth');
const directoryRoutes = require('./routes/directory');

app.use('/api/auth', authRoutes);
app.use('/api/directories', directoryRoutes);

app.get('/', (req, res) => {
  res.status(200).json({ message: 'Backend service is running' });
});

const PORT = process.env.PORT || 5000;

app.listen(PORT, () => console.log(`Server running on port ${PORT}`));

//git test 02 agosto 25
//git test 25 agosto 25
