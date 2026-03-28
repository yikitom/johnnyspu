const { connect } = require('puppeteer-real-browser');

const BASE_URL = 'https://www.klook.com/en-US/destination/c28-tokyo/1-things-to-do/';

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function run(maxPages, onProgress, onBatchReady) {
  onProgress('Klook: Launching real browser (bypassing Cloudflare)...');

  const { page, browser } = await connect({
    headless: false,
    turnstile: true,      // auto-solve Cloudflare Turnstile
    fingerprint: true,     // randomize fingerprint
    args: ['--no-proxy-server', '--lang=en-US'],
  });

  let totalNew = 0;
  let totalDup = 0;

  try {
    for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
      const url = pageNum === 1
        ? `${BASE_URL}?spm=Country.TopDestination_City_LIST&clickId=0dca67bafa`
        : `${BASE_URL}?page=${pageNum}`;
      onProgress(`Klook: Loading page ${pageNum}/${maxPages}...`);

      let loaded = false;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
          loaded = true;
          break;
        } catch (err) {
          onProgress(`Klook: Page ${pageNum} attempt ${attempt}/3 failed: ${err.message}`);
          if (attempt < 3) await delay(5000);
        }
      }

      if (!loaded) {
        onProgress(`Klook: Failed to load page ${pageNum} after 3 attempts, stopping.`);
        break;
      }

      // Wait for rendering + Cloudflare challenge resolution
      await delay(5000 + Math.random() * 3000);

      // Scroll to trigger lazy loading
      await autoScroll(page);
      await delay(3000);

      // Diagnose: what link patterns exist on the page?
      const diagnosis = await page.evaluate(() => {
        const allLinks = document.querySelectorAll('a[href]');
        const patterns = {};
        let productLinkCount = 0;
        const sampleHrefs = [];
        allLinks.forEach(a => {
          const href = a.getAttribute('href') || '';
          if (href.match(/\/(activity|experience|event|attraction)\/\d+/)) {
            productLinkCount++;
            if (sampleHrefs.length < 5) sampleHrefs.push(href);
          }
          const match = href.match(/klook\.com\/[^/]+\/([^/?#]+)/);
          if (match) {
            const seg = match[1];
            patterns[seg] = (patterns[seg] || 0) + 1;
          }
        });
        return { totalLinks: allLinks.length, productLinkCount, sampleHrefs, topPatterns: Object.entries(patterns).sort((a,b)=>b[1]-a[1]).slice(0,10) };
      });

      onProgress(`Klook: Page ${pageNum} - ${diagnosis.totalLinks} links, ${diagnosis.productLinkCount} product links. Samples: ${diagnosis.sampleHrefs.slice(0,3).join(' | ')}`);

      // Extract products
      const products = await page.evaluate((pNum) => {
        const items = [];
        const seen = new Set();

        const productPatterns = [
          /\/activity\/(\d+)/,
          /\/experience\/(\d+)/,
          /\/event\/(\d+)/,
          /\/attraction\/(\d+)/,
        ];

        const allLinks = document.querySelectorAll('a[href]');
        allLinks.forEach((link) => {
          const href = link.getAttribute('href') || '';

          let activityId = null;
          for (const pattern of productPatterns) {
            const m = href.match(pattern);
            if (m) { activityId = m[1]; break; }
          }
          if (!activityId || seen.has(activityId)) return;
          seen.add(activityId);

          // Walk up to card container
          let container = link;
          for (let i = 0; i < 8; i++) {
            if (container.parentElement) container = container.parentElement;
            const cls = (container.className || '').toLowerCase();
            if (cls.includes('card') || cls.includes('item') || cls.includes('product') || cls.includes('result') || cls.includes('activity')) break;
          }

          // Title
          const titleEl = container.querySelector('h3, h2, h4') ||
            container.querySelector('[class*="title" i], [class*="name" i]') ||
            link.querySelector('h3, h2, h4');
          let title = titleEl?.textContent?.trim() || link.textContent?.trim() || '';
          title = title.replace(/\s+/g, ' ').trim();
          if (!title || title.length < 3) return;
          if (title.length > 200) title = title.substring(0, 200);

          // Price
          const allText = container.textContent || '';
          let priceUsd = null;
          let priceCny = null;
          const priceEls = container.querySelectorAll('[class*="price" i], [class*="Price"], [class*="amount" i]');
          priceEls.forEach(el => {
            const t = el.textContent.trim();
            const mUsd = t.match(/(?:US\s*\$|\$|USD)\s*([\d,.]+)/);
            if (mUsd && !priceUsd) priceUsd = parseFloat(mUsd[1].replace(/,/g, ''));
            const mCny = t.match(/(?:CN\s*¥|¥|￥)\s*([\d,]+)/);
            if (mCny && !priceCny) priceCny = parseFloat(mCny[1].replace(/,/g, ''));
          });
          if (!priceUsd && !priceCny) {
            const pm = allText.match(/(?:US\s*\$|\$)\s*([\d,.]+)/);
            if (pm) priceUsd = parseFloat(pm[1].replace(/,/g, ''));
            const pmCny = allText.match(/(?:¥|￥)\s*([\d,]+)/);
            if (pmCny) priceCny = parseFloat(pmCny[1].replace(/,/g, ''));
          }

          // Rating
          let rating = null;
          const ratingEls = container.querySelectorAll('[class*="rating" i], [class*="score" i], [class*="star" i]');
          ratingEls.forEach(el => {
            const m = el.textContent.match(/([\d.]+)/);
            if (m && !rating) {
              const v = parseFloat(m[1]);
              if (v >= 1 && v <= 5) rating = v;
            }
          });

          // Review count
          let reviewCount = null;
          const rcMatches = allText.match(/([\d,]+)\s*(?:reviews?|ratings?|评价)/i)
            || allText.match(/\(([\d,K]+)\+?\)/);
          if (rcMatches) {
            let rc = rcMatches[1].replace(/,/g, '');
            if (rc.includes('K') || rc.includes('k')) {
              reviewCount = Math.round(parseFloat(rc) * 1000);
            } else {
              reviewCount = parseInt(rc, 10);
            }
          }

          // Image
          const imgEl = container.querySelector('img[src*="http"]');
          const imageUrl = imgEl?.src || '';

          // Category
          const catEl = container.querySelector('[class*="category" i], [class*="tag" i]');
          const category = catEl?.textContent?.trim() || '';

          items.push({
            product_id: `KL_AUTO_${activityId}`,
            platform: 'Klook',
            title,
            category: category.substring(0, 50),
            spu_type: '',
            rating,
            review_count: reviewCount,
            price_cny: priceCny,
            price_usd: priceUsd,
            source_url: href.startsWith('http') ? href : `https://www.klook.com${href}`,
            image_url: imageUrl,
            badge: '',
            page_num: pNum,
          });
        });

        return items;
      }, pageNum);

      onProgress(`Klook: Page ${pageNum} found ${products.length} products`);

      if (products.length === 0) {
        if (pageNum === 1) {
          onProgress(`Klook: No products on page 1. If CAPTCHA appeared, please solve it. Waiting 25s then retrying...`);
          await delay(25000);
          await autoScroll(page);
          await delay(3000);
          const retryCount = await page.evaluate(() => {
            const patterns = [/\/activity\/(\d+)/, /\/experience\/(\d+)/, /\/event\/(\d+)/, /\/attraction\/(\d+)/];
            let count = 0;
            document.querySelectorAll('a[href]').forEach(a => {
              if (patterns.some(p => p.test(a.getAttribute('href') || ''))) count++;
            });
            return count;
          });
          if (retryCount > 0) {
            onProgress(`Klook: Found ${retryCount} product links after wait. Retrying...`);
            pageNum--;
            continue;
          }
          onProgress(`Klook: Still no products, stopping.`);
        }
        break;
      }

      const result = await onBatchReady(products);
      totalNew += result.written;
      totalDup += result.duplicates;
      onProgress(`Klook: Page ${pageNum} done - ${result.written} new, ${result.duplicates} dups (Total: ${totalNew} new, ${totalDup} dups)`);

      if (pageNum < maxPages) {
        const waitTime = 3000 + Math.random() * 4000;
        onProgress(`Klook: Waiting ${(waitTime / 1000).toFixed(1)}s...`);
        await delay(waitTime);
      }
    }
  } finally {
    await browser.close();
  }

  onProgress(`Klook: Completed! ${totalNew} new records, ${totalDup} duplicates`);
}

async function autoScroll(page) {
  await page.evaluate(async () => {
    await new Promise((resolve) => {
      let totalHeight = 0;
      const distance = 400;
      const timer = setInterval(() => {
        window.scrollBy(0, distance);
        totalHeight += distance;
        if (totalHeight >= document.body.scrollHeight - window.innerHeight) {
          clearInterval(timer);
          resolve();
        }
      }, 300);
      setTimeout(() => { clearInterval(timer); resolve(); }, 12000);
    });
  });
}

module.exports = { run };
