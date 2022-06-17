const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');
const { addReviews, addPhotos, addCharacteristics, updateCharacteristics } = require('./etlSetupHelpers.js');

const reviewsCSV = '../../api-data/raw-data/reviewsRaw.csv';
const photosCSV = '../../api-data/raw-data/photosRaw.csv';
const characteristicsCSV = '../../api-data/raw-data/characteristicsRaw.csv';
const characteristicReviewsCSV = '../../api-data/raw-data/characteristicReviewsRaw.csv';

// Need to add a port number?
mongoose.connect('mongodb://${user}:${pwd}@127.0.0.1:27017/reviews',
  {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(() => {
    console.log(`MongoDB Connected!`);
  })
  .catch((err) => {
    console.log(`MongoDB ERR ${err}`);
  });

// addReviews(reviewsCSV);
// addPhotos(photosCSV);
// addCharacteristics(characteristicsCSV);
// updateCharacteristics(characteristicReviewsCSV);