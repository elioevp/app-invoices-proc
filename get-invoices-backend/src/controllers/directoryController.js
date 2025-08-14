
const blobServiceClient = require('../config/azure');

exports.getDirectories = async (req, res) => {
  const containerClient = blobServiceClient.getContainerClient(process.env.AZURE_STORAGE_CONTAINER_NAME);
  try {
    let directories = new Set();
    for await (const blob of containerClient.listBlobsFlat({ prefix: `${req.userId}/` })) {
      const parts = blob.name.split('/');
      if (parts.length > 1 && parts[1] !== '.placeholder') { // Ensure it's a directory and not the placeholder file
        directories.add(parts[1]);
      }
    }
    res.json(Array.from(directories));
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

exports.createDirectory = async (req, res) => {
  const { directoryName } = req.body;
  const containerClient = blobServiceClient.getContainerClient(process.env.AZURE_STORAGE_CONTAINER_NAME);
  const blockBlobClient = containerClient.getBlockBlobClient(`${req.userId}/${directoryName}/.placeholder`);
  try {
    await blockBlobClient.upload("", 0);
    res.status(201).json({ message: 'Directory created successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

exports.uploadFile = async (req, res) => {
    const { directoryName } = req.params;
    const file = req.file;

    if (!file) {
        return res.status(400).json({ message: 'No file uploaded' });
    }

    const containerClient = blobServiceClient.getContainerClient(process.env.AZURE_STORAGE_CONTAINER_NAME);
    const blockBlobClient = containerClient.getBlockBlobClient(`${req.userId}/${directoryName}/${file.originalname}`);

    try {
        await blockBlobClient.upload(file.buffer, file.size);
        res.status(201).json({ message: 'File uploaded successfully' });
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
};

exports.getFilesInDirectory = async (req, res) => {
  const { directoryName } = req.params;
  const containerClient = blobServiceClient.getContainerClient(process.env.AZURE_STORAGE_CONTAINER_NAME);
  const files = [];

  try {
    for await (const blob of containerClient.listBlobsFlat({ prefix: `${req.userId}/${directoryName}/` })) {
      if (blob.name !== `${req.userId}/${directoryName}/.placeholder`) {
        files.push({
          name: blob.name.split('/').pop(),
          url: `https://${process.env.AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/${process.env.AZURE_STORAGE_CONTAINER_NAME}/${blob.name}`,
        });
      }
    }
    res.json(files);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};

exports.deleteFile = async (req, res) => {
  const { directoryName, fileName } = req.params;
  const containerClient = blobServiceClient.getContainerClient(process.env.AZURE_STORAGE_CONTAINER_NAME);
  const blockBlobClient = containerClient.getBlockBlobClient(`${req.userId}/${directoryName}/${fileName}`);

  try {
    await blockBlobClient.delete();
    res.status(200).json({ message: 'File deleted successfully' });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
};
