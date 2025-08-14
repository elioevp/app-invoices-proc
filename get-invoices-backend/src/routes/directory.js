
const express = require('express');
const router = express.Router();
const directoryController = require('../controllers/directoryController');
const authMiddleware = require('../middleware/authMiddleware');
const multer = require('multer');

const upload = multer({ storage: multer.memoryStorage() });

router.get('/', authMiddleware, directoryController.getDirectories);
router.post('/', authMiddleware, directoryController.createDirectory);
router.post('/upload/:directoryName', [authMiddleware, upload.single('file')], directoryController.uploadFile);
router.get('/:directoryName/files', authMiddleware, directoryController.getFilesInDirectory);
router.delete('/:directoryName/files/:fileName', authMiddleware, directoryController.deleteFile);

module.exports = router;
