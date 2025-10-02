// queries.js
const { MongoClient } = require('mongodb');

const uri = process.env.MONGODB_URI || "mongodb://127.0.0.1:27017";
const client = new MongoClient(uri);
const dbName = 'plp_bookstore';
const collName = 'books';

async function main() {
  const client = new MongoClient(uri);
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    const db = client.db(dbName);
    const books = db.collection(collName);


    // --- Task 2: Basic CRUD examples Operations---

    // 2.1 Find all books in a specific genre
    async function findByGenre(genre) {
      const docs = await books.find({ genre }).toArray();
      console.log(`\nBooks in genre "${genre}":`, docs);
      return docs;
    }

    // 2.2 Find books published after a certain year
    async function findPublishedAfter(year) {
      const docs = await books.find({ published_year: { $gt: year } }).toArray();
      console.log(`\nBooks published after ${year}:`, docs);
      return docs;
    }

    // 2.3 Find books by a specific author
    async function findByAuthor(author) {
      const docs = await books.find({ author }).toArray();
      console.log(`\nBooks by "${author}":`, docs);
      return docs;
    }

    // 2.4 Update the price of a specific book
    async function updatePrice(title, newPrice) {
      const r = await books.updateOne({ title }, { $set: { price: newPrice } });
      console.log(`\nUpdated price for "${title}": matched=${r.matchedCount}, modified=${r.modifiedCount}`);
      return r;
    }

    // 2.5 Delete a book by its title
    async function deleteByTitle(title) {
      const r = await books.deleteOne({ title });
      console.log(`\nDeleted "${title}": deletedCount=${r.deletedCount}`);
      return r;
    }


    // --- Task 3: Advanced Queries ---

    // 3.1 Find books that are in stock and published after a given year (with projection)
    async function inStockAfter(year) {
      const projection = { title: 1, author: 1, price: 1, _id: 0 };
      const docs = await books.find({ in_stock: true, published_year: { $gt: year } }, { projection }).toArray();
      console.log(`\nIn-stock books after ${year}:`, docs);
      return docs;
    }

    // 3.2 Sorting by price (asc / desc)
    async function sortedByPrice(order = 'asc') {
      const sort = order === 'asc' ? { price: 1 } : { price: -1 };
      const docs = await books.find({}, { projection: { title: 1, price: 1, _id: 0 } }).sort(sort).toArray();
      console.log(`\nBooks sorted by price (${order}):`, docs);
      return docs;
    }

    // 3.3 Pagination: page (1-based), pageSize
    async function getPage(page = 1, pageSize = 5) {
      const skip = (page - 1) * pageSize;
      const docs = await books.find({}, { projection: { title: 1, author: 1, _id: 0 } }).skip(skip).limit(pageSize).toArray();
      console.log(`\nPage ${page} (size ${pageSize}):`, docs);
      return docs;
    }


    // --- Task 4: Aggregation Pipelines ---

    // 4.1 Average price of books by genre
    async function avgPriceByGenre() {
      const pipeline = [
        { $group: { _id: "$genre", avgPrice: { $avg: "$price" }, count: { $sum: 1 } } },
        { $sort: { avgPrice: -1 } }
      ];
      const res = await books.aggregate(pipeline).toArray();
      console.log(`\nAverage price by genre:`, res);
      return res;
    }

    // 4.2 Author with the most books in the collection
    async function authorWithMostBooks() {
      const pipeline = [
        { $group: { _id: "$author", count: { $sum: 1 } } },
        { $sort: { count: -1 } },
        { $limit: 1 }
      ];
      const res = await books.aggregate(pipeline).toArray();
      console.log(`\nAuthor with most books:`, res);
      return res;
    }

    // 4.3 Group books by publication decade and count
    async function groupByDecade() {
      const pipeline = [
        { $project: { decade: { $multiply: [{ $floor: { $divide: ["$published_year", 10] } }, 10] } } },
        { $group: { _id: "$decade", count: { $sum: 1 } } },
        { $sort: { _id: 1 } }
      ];
      const res = await books.aggregate(pipeline).toArray();
      console.log(`\nBooks grouped by decade:`, res);
      return res;
    }


    // --- Task 5: Indexing and explain() ---

    // explain before and after creating indexes
    async function explainIndexEffect(testQuery = { title: "1984" }) {
      console.log('\nRunning explain() BEFORE creating index...');
      const before = await books.find(testQuery).explain("executionStats");
      console.log('explain BEFORE (summary):', {
        nReturned: before.executionStats.nReturned,
        totalKeysExamined: before.executionStats.totalKeysExamined,
        totalDocsExamined: before.executionStats.totalDocsExamined,
        executionTimeMillis: before.executionStats.executionTimeMillis
      });

      console.log('\nCreating index on title and compound index on {author, published_year} ...');
      await books.createIndex({ title: 1 });
      await books.createIndex({ author: 1, published_year: -1 });

      console.log('\nRunning explain() AFTER creating index...');
      const after = await books.find(testQuery).explain("executionStats");
      console.log('explain AFTER (summary):', {
        nReturned: after.executionStats.nReturned,
        totalKeysExamined: after.executionStats.totalKeysExamined,
        totalDocsExamined: after.executionStats.totalDocsExamined,
        executionTimeMillis: after.executionStats.executionTimeMillis
      });

      return { before, after };
    }

    // --- Run the functions with sample inputs (you can edit these) ---
    await findByGenre('Fiction');
    await findPublishedAfter(1950);
    await findByAuthor('George Orwell');
    await updatePrice('1984', 11.99);            // example price update
    // await deleteByTitle('Bad Book');         // uncomment to test delete

    await inStockAfter(2010);
    await sortedByPrice('asc');
    await sortedByPrice('desc');
    await getPage(1, 5);
    await getPage(2, 5);

    await avgPriceByGenre();
    await authorWithMostBooks();
    await groupByDecade();

    await explainIndexEffect({ title: '1984' });

  } catch (err) {
    console.error('Error in queries.js:', err);
  } finally {
    await client.close();
    console.log('\nDisconnected from MongoDB');
  }
}

main();
