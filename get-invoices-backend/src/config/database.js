
const mysql = require('mysql2/promise');
const fs = require('fs');
require('dotenv').config();

const dbConfig = {
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_DATABASE,
  port: process.env.DB_PORT || 3306,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  ssl: {
    rejectUnauthorized: true
  }
};

// Use local SSL certificate only if the path is defined in .env
if (process.env.DB_SSL_CA_PATH) {
  dbConfig.ssl.ca = fs.readFileSync(process.env.DB_SSL_CA_PATH);
}

const pool = mysql.createPool(dbConfig);

module.exports = pool;
