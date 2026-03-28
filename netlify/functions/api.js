const Airtable = require('airtable');

const BASE_ID = process.env.AIRTABLE_BASE_ID || 'appBn4rAsuq14VeDf';
const TABLE_ID = process.env.AIRTABLE_TABLE_ID || 'tblKlMT96Z03jLl1J';

let base;
function getTable() {
  if (!base) {
    base = new Airtable({ apiKey: process.env.AIRTABLE_PAT }).base(BASE_ID);
  }
  return base(TABLE_ID);
}

exports.handler = async (event) => {
  const path = event.path.replace('/.netlify/functions/api/', '').replace('/api/', '');
  const headers = {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
  };

  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 200, headers, body: '' };
  }

  try {
    if (path === 'stats') {
      return await handleStats(headers);
    }
    if (path === 'scraper-status') {
      return {
        statusCode: 200,
        headers,
        body: JSON.stringify({
          getyourguide: { running: false, progress: 'Scraping runs locally only', lastRun: null, newRecords: 0, duplicates: 0 },
          klook: { running: false, progress: 'Scraping runs locally only', lastRun: null, newRecords: 0, duplicates: 0 },
        }),
      };
    }
    return { statusCode: 404, headers, body: JSON.stringify({ error: 'Not found' }) };
  } catch (err) {
    console.error('Function error:', err);
    return { statusCode: 500, headers, body: JSON.stringify({ error: err.message }) };
  }
};

async function handleStats(headers) {
  const table = getTable();
  const records = [];

  await new Promise((resolve, reject) => {
    table.select({
      fields: ['platform', 'category', 'spu_type', 'rating', 'review_count', 'price_cny', 'page_num'],
    }).eachPage(
      (pageRecords, fetchNextPage) => {
        pageRecords.forEach(r => records.push(r.fields));
        fetchNextPage();
      },
      (err) => { if (err) reject(err); else resolve(); }
    );
  });

  const stats = {
    total: records.length,
    byPlatform: {},
    byCategory: {},
    bySpu: {},
    priceRanges: { '0-100': 0, '100-300': 0, '300-500': 0, '500-1000': 0, '1000+': 0 },
    ratingDistribution: { '4.8+': 0, '4.5-4.8': 0, '4.0-4.5': 0, '<4.0': 0, 'N/A': 0 },
  };

  records.forEach(r => {
    const plat = r.platform || 'Unknown';
    stats.byPlatform[plat] = (stats.byPlatform[plat] || 0) + 1;

    const cat = r.category || 'Unknown';
    stats.byCategory[cat] = (stats.byCategory[cat] || 0) + 1;

    const spu = r.spu_type || 'Unknown';
    stats.bySpu[spu] = (stats.bySpu[spu] || 0) + 1;

    const price = r.price_cny || 0;
    if (price <= 100) stats.priceRanges['0-100']++;
    else if (price <= 300) stats.priceRanges['100-300']++;
    else if (price <= 500) stats.priceRanges['300-500']++;
    else if (price <= 1000) stats.priceRanges['500-1000']++;
    else stats.priceRanges['1000+']++;

    const rating = r.rating;
    if (!rating) stats.ratingDistribution['N/A']++;
    else if (rating >= 4.8) stats.ratingDistribution['4.8+']++;
    else if (rating >= 4.5) stats.ratingDistribution['4.5-4.8']++;
    else if (rating >= 4.0) stats.ratingDistribution['4.0-4.5']++;
    else stats.ratingDistribution['<4.0']++;
  });

  return { statusCode: 200, headers, body: JSON.stringify(stats) };
}
