// models/Review.js
const mongoose = require('mongoose');

const reviewSchema = new mongoose.Schema({
  age: {
    type: Number,
    required: true,
  },
  job: {
    type: String,
    required: true,
  },
  sector: {
    type: String,
    required: true,
  },
  monthlyIncome: {
    type: Number,
    required: true,
  },
  gender: {
    type: String,
    required: true,
  },
  civilState: {
    type: String,
    required: true,
  },
  familyMembers: {
    type: Number,
    required: true,
  },
  vehicleType: {
    type: String,
    required: true,
  },
  vehicleBrand: {
    type: String,
    required: true,
  },
  fuelType: {
    type: String,
    required: true,
  },
  image: {
    type: String,
    required: false, // The URL of the uploaded image
  },
}, { timestamps: true });

module.exports = mongoose.model('Review', reviewSchema);