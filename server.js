require('dotenv').config();
const express = require('express');
const path = require('path');
const Airtable = require('airtable');

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const BASE_ID = process.env.AIRTABLE_BASE_ID || 'appBn4rAsuq14VeDf';
const TABLE_ID = process.env.AIRTABLE_TABLE_ID || 'tblKlMT96Z03jLl1J';

const base = new Airtable({ apiKey: process.env.AIRTABLE_PAT }).base(BASE_ID);
const table = base(TABLE_ID);

// Field IDs mapping
const FIELDS = {
  product_id: 'fldOZBTI2j9Mvyxzm',
  platform: 'fldz7ta0tzK6dFu4Z',
  title: 'fld7ktFARMRfn463y',
  category: 'fldlRweVxTs5oXTDE',
  spu_type: 'fldwMhz2G7NazO5gs',
  location: 'fldCkV5VCd91hlusD',
  rating: 'fld2Yf2zHIakloXdz',
  review_count: 'fldaqEnrGP3gw2ZV2',
  price_cny: 'fldP5ZzMrK6KVwl8v',
  price_usd: 'fldw6vgo22IqXpBWK',
  duration: 'flddN3JMta9R0Lecc',
  badge: 'fldGceGypl7WgXMXa',
  demand_level: 'flduG0DaRmcAPB8NY',
  source_url: 'fldBvFpNSK57YNo5T',
  image_url: 'fldSm68bCZKon0yIS',
  page_num: 'fld9cVdIQoUXy4vq4',
  fetched_at: 'fldAptJHasP6uooVX',
};

// Track running scrapers
const scraperStatus = {
  getyourguide: { running: false, progress: '', lastRun: null, newRecords: 0, duplicates: 0 },
  klook: { running: false, progress: '', lastRun: null, newRecords: 0, duplicates: 0 },
};

// ========== API: Get all records stats ==========
app.get('/api/stats', async (req, res) => {
  try {
    const records = [];
    await table.select({ fields: ['platform', 'category', 'spu_type', 'rating', 'review_count', 'price_cny', 'page_num'] })
      .eachPage((pageRecords, fetchNextPage) => {
        pageRecords.forEach(r => records.push(r.fields));
        fetchNextPage();
      });

    const stats = {
      total: records.length,
      byPlatform: {},
      byCategory: {},
      bySpu: {},
      priceRanges: { '0-100': 0, '100-300': 0, '300-500': 0, '500-1000': 0, '1000+': 0 },
      ratingDistribution: { '4.8+': 0, '4.5-4.8': 0, '4.0-4.5': 0, '<4.0': 0, 'N/A': 0 },
      topReviewed: [],
    };

    records.forEach(r => {
      // By platform
      const plat = r.platform || 'Unknown';
      stats.byPlatform[plat] = (stats.byPlatform[plat] || 0) + 1;

      // By category
      const cat = r.category || 'Unknown';
      stats.byCategory[cat] = (stats.byCategory[cat] || 0) + 1;

      // By SPU type
      const spu = r.spu_type || 'Unknown';
      stats.bySpu[spu] = (stats.bySpu[spu] || 0) + 1;

      // Price ranges (CNY)
      const price = r.price_cny || 0;
      if (price <= 100) stats.priceRanges['0-100']++;
      else if (price <= 300) stats.priceRanges['100-300']++;
      else if (price <= 500) stats.priceRanges['300-500']++;
      else if (price <= 1000) stats.priceRanges['500-1000']++;
      else stats.priceRanges['1000+']++;

      // Rating distribution
      const rating = r.rating;
      if (!rating) stats.ratingDistribution['N/A']++;
      else if (rating >= 4.8) stats.ratingDistribution['4.8+']++;
      else if (rating >= 4.5) stats.ratingDistribution['4.5-4.8']++;
      else if (rating >= 4.0) stats.ratingDistribution['4.0-4.5']++;
      else stats.ratingDistribution['<4.0']++;
    });

    res.json(stats);
  } catch (err) {
    console.error('Stats error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== API: Get all existing product_ids for dedup ==========
app.get('/api/existing-ids', async (req, res) => {
  try {
    const ids = new Set();
    const titles = new Set();
    await table.select({ fields: ['product_id', 'title'] })
      .eachPage((records, fetchNextPage) => {
        records.forEach(r => {
          if (r.fields.product_id) ids.add(r.fields.product_id);
          if (r.fields.title) titles.add(r.fields.title);
        });
        fetchNextPage();
      });
    res.json({ ids: [...ids], titles: [...titles] });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// ========== API: Write records (with dedup) ==========
app.post('/api/records', async (req, res) => {
  try {
    const { records: newRecords } = req.body;
    if (!newRecords || !newRecords.length) {
      return res.json({ written: 0, duplicates: 0 });
    }

    // Fetch existing product_ids and titles for dedup
    const existingIds = new Set();
    const existingTitles = new Set();
    await table.select({ fields: ['product_id', 'title'] })
      .eachPage((records, fetchNextPage) => {
        records.forEach(r => {
          if (r.fields.product_id) existingIds.add(r.fields.product_id);
          if (r.fields.title) existingTitles.add(r.fields.title.trim());
        });
        fetchNextPage();
      });

    // Filter out duplicates by product_id and title
    const toWrite = newRecords.filter(r => {
      const pid = r.product_id;
      const title = (r.title || '').trim();
      if (existingIds.has(pid)) return false;
      if (title && existingTitles.has(title)) return false;
      return true;
    });

    const duplicates = newRecords.length - toWrite.length;

    // Write in batches of 10
    let written = 0;
    for (let i = 0; i < toWrite.length; i += 10) {
      const batch = toWrite.slice(i, i + 10).map(r => ({
        fields: {
          [FIELDS.product_id]: r.product_id,
          [FIELDS.platform]: r.platform,
          [FIELDS.title]: r.title,
          [FIELDS.category]: r.category || '',
          [FIELDS.spu_type]: r.spu_type || '',
          [FIELDS.location]: r.location || '',
          [FIELDS.rating]: r.rating || null,
          [FIELDS.review_count]: r.review_count || null,
          [FIELDS.price_cny]: r.price_cny || null,
          [FIELDS.price_usd]: r.price_usd || null,
          [FIELDS.duration]: r.duration || '',
          [FIELDS.badge]: r.badge || '',
          [FIELDS.demand_level]: r.demand_level || '',
          [FIELDS.source_url]: r.source_url || '',
          [FIELDS.image_url]: r.image_url || '',
          [FIELDS.page_num]: r.page_num || null,
          [FIELDS.fetched_at]: new Date().toISOString(),
        }
      }));
      await table.create(batch);
      written += batch.length;
    }

    res.json({ written, duplicates, total: newRecords.length });
  } catch (err) {
    console.error('Write error:', err);
    res.status(500).json({ error: err.message });
  }
});

// ========== API: Scraper status ==========
app.get('/api/scraper-status', (req, res) => {
  res.json(scraperStatus);
});

// ========== API: Trigger scraping ==========
app.post('/api/scrape/:platform', async (req, res) => {
  const { platform } = req.params;
  const { maxPages = 10 } = req.body;

  if (!['getyourguide', 'klook'].includes(platform)) {
    return res.status(400).json({ error: 'Invalid platform' });
  }

  if (scraperStatus[platform].running) {
    return res.status(409).json({ error: `${platform} scraper is already running` });
  }

  scraperStatus[platform] = { running: true, progress: 'Starting...', lastRun: new Date().toISOString(), newRecords: 0, duplicates: 0 };
  res.json({ message: `${platform} scraper started`, maxPages });

  // Run scraper in background
  try {
    const scraper = require(`./scrapers/${platform}`);
    await scraper.run(maxPages, (progress) => {
      scraperStatus[platform].progress = progress;
    }, async (records) => {
      // Write batch to Airtable with dedup
      if (!records.length) return { written: 0, duplicates: 0 };

      const existingIds = new Set();
      const existingTitles = new Set();
      await table.select({ fields: ['product_id', 'title'] })
        .eachPage((recs, next) => {
          recs.forEach(r => {
            if (r.fields.product_id) existingIds.add(r.fields.product_id);
            if (r.fields.title) existingTitles.add(r.fields.title.trim());
          });
          next();
        });

      const toWrite = records.filter(r => {
        if (existingIds.has(r.product_id)) return false;
        if (r.title && existingTitles.has(r.title.trim())) return false;
        return true;
      });

      const dups = records.length - toWrite.length;
      let written = 0;
      for (let i = 0; i < toWrite.length; i += 10) {
        const batch = toWrite.slice(i, i + 10).map(r => ({
          fields: {
            [FIELDS.product_id]: r.product_id,
            [FIELDS.platform]: r.platform,
            [FIELDS.title]: r.title,
            [FIELDS.category]: r.category || '',
            [FIELDS.spu_type]: r.spu_type || '',
            [FIELDS.location]: r.location || '',
            [FIELDS.rating]: r.rating || null,
            [FIELDS.review_count]: r.review_count || null,
            [FIELDS.price_cny]: r.price_cny || null,
            [FIELDS.price_usd]: r.price_usd || null,
            [FIELDS.duration]: r.duration || '',
            [FIELDS.badge]: r.badge || '',
            [FIELDS.demand_level]: r.demand_level || '',
            [FIELDS.source_url]: r.source_url || '',
            [FIELDS.image_url]: r.image_url || '',
            [FIELDS.page_num]: r.page_num || null,
            [FIELDS.fetched_at]: new Date().toISOString(),
          }
        }));
        await table.create(batch);
        written += batch.length;
      }

      scraperStatus[platform].newRecords += written;
      scraperStatus[platform].duplicates += dups;
      return { written, duplicates: dups };
    });

    scraperStatus[platform].running = false;
    scraperStatus[platform].progress = 'Completed';
  } catch (err) {
    console.error(`${platform} scraper error:`, err);
    scraperStatus[platform].running = false;
    scraperStatus[platform].progress = `Error: ${err.message}`;
  }
});

const PORT = process.env.PORT || 3456;
app.listen(PORT, () => {
  console.log(`Dashboard running at http://localhost:${PORT}`);
});
