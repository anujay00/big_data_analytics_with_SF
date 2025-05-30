const express = require('express');
const mongoose = require('mongoose');
const multer = require('multer');
const path = require('path');
const Review = require('./models/Review');
const dotenv = require('dotenv');
const cors = require('cors');
const fs = require('fs');

// Load environment variables
dotenv.config();

const app = express();
app.use(express.json()); // For parsing JSON data

// CORS configuration
app.use(cors({
  origin: true, // Allow requests from any origin
  methods: 'GET,POST,DELETE', // Allow GET, POST, and DELETE methods
  credentials: true, // Enable cookies to be sent
}));

// Create uploads directory if it doesn't exist
const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)) {
  fs.mkdirSync(uploadsDir, { recursive: true });
}

// Serve static files (uploaded images)
app.use('/uploads', express.static('uploads'));

// MongoDB Atlas connection using Mongoose
mongoose.connect(process.env.MONGO_URI, {
  useNewUrlParser: true,
  useUnifiedTopology: true,
})
.then(() => console.log('MongoDB connected'))
.catch((err) => console.error('MongoDB connection error:', err));

// Multer config for handling file uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => cb(null, 'uploads/'), // Save files to /uploads
  filename: (req, file, cb) => cb(null, Date.now() + path.extname(file.originalname)),
});
const upload = multer({ storage });

// API Routes

// Add a new review, with or without image
app.post('/api/reviews', upload.single('image'), async (req, res) => {
  try {
    const { 
      age, 
      job, 
      sector, 
      monthlyIncome, 
      gender, 
      civilState, 
      familyMembers, 
      vehicleType, 
      vehicleBrand, 
      fuelType 
    } = req.body;

    // Check if an image was uploaded
    const image = req.file ? `/uploads/${req.file.filename}` : null;

    const newReview = new Review({ 
      age, 
      job, 
      sector, 
      monthlyIncome, 
      gender, 
      civilState, 
      familyMembers, 
      vehicleType, 
      vehicleBrand, 
      fuelType,
      image 
    });
    
    await newReview.save();

    res.status(201).json({ message: 'Review added successfully' });
  } catch (error) {
    console.error('Error adding review:', error);
    res.status(500).json({ message: 'Error adding review', error: error.message });
  }
});

// Get all reviews
app.get('/api/reviews', async (req, res) => {
  try {
    const reviews = await Review.find().sort({ createdAt: -1 }); // Sort by newest first
    res.json(reviews);
  } catch (error) {
    res.status(500).json({ message: 'Error fetching reviews', error: error.message });
  }
});

// Delete a review by ID
app.delete('/api/reviews/:id', async (req, res) => {
  try {
    const { id } = req.params;
    const review = await Review.findById(id);

    if (!review) {
      return res.status(404).json({ message: 'Review not found' });
    }

    // Delete the review from the database
    await Review.findByIdAndDelete(id);

    // Remove the image file from the server if it exists
    if (review.image) {
      const imagePath = path.join(__dirname, review.image.replace('/uploads/', 'uploads/'));
      if (fs.existsSync(imagePath)) {
        fs.unlink(imagePath, (err) => {
          if (err) {
            console.error('Error deleting image file:', err);
          }
        });
      }
    }

    res.json({ message: 'Review deleted successfully' });
  } catch (error) {
    res.status(500).json({ message: 'Error deleting review', error: error.message });
  }
});

// Start the server
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));