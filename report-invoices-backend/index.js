require('dotenv').config();
const express = require('express');
const { CosmosClient } = require('@azure/cosmos');

// Check for required environment variables
const endpoint = process.env.COSMOS_ENDPOINT;
const key = process.env.COSMOS_KEY;
const databaseId = process.env.COSMOS_DATABASE;
const containerId = process.env.COSMOS_CONTAINER;

if (!endpoint || !key || !databaseId || !containerId) {
  console.error('FATAL ERROR: Missing one or more required environment variables for Cosmos DB.');
  process.exit(1);
}

// Initialize Cosmos DB client
const cosmosClient = new CosmosClient({ endpoint, key });
const database = cosmosClient.database(databaseId);
const container = database.container(containerId);

// Initialize Express app
const app = express();
const cors = require('cors');
app.use(cors());
app.use(express.json());

// --- API Endpoint --- //
app.get('/api/report', async (req, res) => {
  console.log(`Received request for report: ${req.url}`);

  const { username, directorio } = req.query;

  if (!username || !directorio) {
    return res.status(400).json({
      message: "Please provide 'username' and 'directorio' in the query parameters."
    });
  }

  try {
    // Define the SQL query
    const querySpec = {
      query: 'SELECT * FROM c WHERE c.username = @username AND c.directorio = @directorio',
      parameters: [
        { name: '@username', value: username },
        { name: '@directorio', value: directorio },
      ],
    };

    // Execute the query
    const { resources: items } = await container.items.query(querySpec).fetchAll();

    // Calculate the total amount
    const monto_total_calculado = items.reduce((sum, item) => sum + (item.montoTotal || 0), 0);

    // Prepare the response
    const resultado = {
      username,
      directorio,
      numero_facturas: items.length,
      monto_total_calculado,
      facturas: items,
    };

    res.status(200).json(resultado);

  } catch (error) {
    console.error('Error querying Cosmos DB:', error);
    res.status(500).json({ message: 'An unexpected error occurred while processing the request.' });
  }
});

// --- Server Start --- //
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});

// end
