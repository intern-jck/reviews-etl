const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const mongoose = require('mongoose');

const reviewsLength = 5774952;
const photosLength = 2742540;
const chracteristicsLength = 3347679;
const reviewChracteristicsLength = 19327575;

const BasicReview = require('./ReviewModel.js');

const addReviews = (csvPath) => {

  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    // Need to store multiple inc operations in and object.
    const incUpdates = {};

    // Increment meta.ratings by 1
    incUpdates['meta.ratings.' + row.rating] = 1;

    // Increment meta.recommended by 1
    if (row.recommend === 'false') {
      incUpdates['meta.recommended.0'] = 1;
    } else if (row.recommend === 'true') {
      incUpdates['meta.recommended.1'] = 1;
    }

    // Store everything in an operation
    const reviewOP = {
      updateOne: {
        'filter': { 'product_id': row.product_id},
        'update': {
          '$push': {
            'results': {
              'id':  row.id,
              'rating':  row.rating,
              'date': row.date,
              'summary': row.summary,
              'body': row.body,
              'recommend': row.recommend,
              'reported': row.reported,
              'reviewer_name': row.reviewer_name,
              'reviewer_email': row.reviewer_email,
              'response': row.response,
              'helpfulness': row.helpfulness,
            }
          },
          '$inc': incUpdates
        },
        'upsert': true,
      }
    }

    // Add it to the queue.
    operations.push(reviewOP);

    if (operations.length > 10000) {
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ reviewsLength) * 100)}%`);
      BasicReview.bulkWrite(operations);
      operations = [];
    }

  })
  .on('end', (rowCount) => {

    BasicReview.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);

    // Hacky way to keep track of reviews
    const filter = { 'product_id': 0};
    const update = { '$set': { 'review_count': rowCount } }
    const options = {
      new: true,
      strict: false,
      upsert: true
    }

    BasicReview.findOneAndUpdate(filter, update, options).then((doc) => ( console.log(doc)));

  });
};

const addPhotos = (csvPath) => {

  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const photosOP = {
      updateOne: {
        'filter': { "results.id": row.review_id },
        'update': {
          '$push': { 'results.$.photos': {
            'id': row.id,
            'url': row.url,
          }},
          'upsert': true
        },
      }
    };

    operations.push(photosOP);

    if (operations.length > 2500) {
      BasicReview.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ photosLength) * 100)}%`);
      operations = [];
    }

  })
  .on('end', (rowCount) => {
    BasicReview.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });
};

const addCharacteristics = (csvPath) => {

  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const newCharacteristic = {};
    newCharacteristic['meta.characteristics.' + row.id] = {
      name: row.name,
      value: []
    };

    const updateOne = {
      updateOne: {
        'filter': { 'product_id': row.product_id },
        'update': { '$set': newCharacteristic },
      }
    };

    operations.push(updateOne)

    if(operations.length > 2500) {
      BasicReview.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ chracteristicsLength) * 100)}%`);
      operations = [];
    }

  })
  .on('end', (rowCount) => {
    BasicReview.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`);
  });

};

const updateCharacteristics = (csvPath) => {

  let operations = [];
  const t0 = performance.now();

  fs.createReadStream(path.resolve(__dirname, csvPath))
  .pipe(csv.parse({ headers: true }))
  .on('error', error => console.error(error))
  .on('data', (row) => {

    const updateCharacteristic = {};
    updateCharacteristic['meta.characteristics.' + row.characteristic_id + '.value'] = parseInt(row.value);

    const updateOne = {
      updateOne: {
        'filter': { 'results.id': row.review_id },
        'update': { '$push': updateCharacteristic },
      }
    };

    operations.push(updateOne)

    if(operations.length > 2500) {
      BasicReview.bulkWrite(operations);
      const tEnd = performance.now();
      console.log(`Bulk Update @ ${Math.round(tEnd - t0)} : ${Math.round((parseInt(row.id)/ reviewChracteristicsLength) * 100)}%`);
      operations = [];
    }

  })
  .on('end', (rowCount) => {
    BasicReview.bulkWrite(operations);
    const tEnd = performance.now();
    console.log(`Added ${rowCount} rows in ${Math.round(tEnd - t0)}`)
  });

};


module.exports = {
  addReviews,
  addPhotos,
  addCharacteristics,
  updateCharacteristics
};