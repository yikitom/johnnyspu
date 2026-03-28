const { connect } = require('puppeteer-real-browser');

const BASE_URL = 'https://www.getyourguide.com/zh-cn/tokyo-l193/';

function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function run(maxPages, onProgress, onBatchReady) {
  onProgress('GetYourGuide: Launching real browser (bypassing Cloudflare)...');

  const { page, browser } = await connect({
    headless: false,
    turnstile: true,
    fingerprint: true,
    args: ['--no-proxy-server', '--lang=zh-CN'],
  });

  let totalNew = 0;
  let totalDup = 0;

  try {
    for (let pageNum = 1; pageNum <= maxPages; pageNum++) {
      const url = pageNum === 1 ? BASE_URL : `${BASE_URL}?page=${pageNum}`;
      onProgress(`GetYourGuide: Loading page ${pageNum}/${maxPages}...`);

      let loaded = false;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });
          loaded = true;
          break;
        } catch (err) {
          onProgress(`GetYourGuide: Page ${pageNum} attempt ${attempt}/3 failed: ${err.message}`);
          if (attempt < 3) await delay(5000);
        }
      }

      if (!loaded) {
        onProgress(`GetYourGuide: Failed to load page ${pageNum} after 3 attempts, stopping.`);
        break;
      }

      await delay(5000 + Math.random() * 3000);

      // Scroll to load content
      await page.evaluate(async () => {
        for (let i = 0; i < 6; i++) {
          window.scrollBy(0, 600);
          await new Promise(r => setTimeout(r, 500));
        }
        window.scrollTo(0, 0);
      });
      await delay(2000);

      // Wait for card content
      try {
        await page.waitForSelector('a[href*="-t"], a[href*="/activity/"]', { timeout: 15000 });
      } catch {
        onProgress(`GetYourGuide: No card links on page ${pageNum}. Checking...`);
      }

      // Extract products
      const products = await page.evaluate((pNum) => {
        const items = [];
        const seen = new Set();

        const allLinks = document.querySelectorAll('a[href*="-t"], a[href*="/activity/"]');
        allLinks.forEach((link) => {
          const href = link.getAttribute('href') || '';
          if (!href.includes('-t') && !href.includes('/activity/')) return;
          if (seen.has(href)) return;
          seen.add(href);

          let container = link;
          for (let i = 0; i < 6; i++) {
            if (container.parentElement) container = container.parentElement;
            const cls = container.className || '';
            if (cls.includes('card') || cls.includes('Card') || cls.includes('activity') || cls.includes('vertical-activity')) break;
          }

          const titleEl = container.querySelector('h3, h2, h4') ||
            container.querySelector('[class*="title"], [class*="Title"]') ||
            link.querySelector('h3, h2, h4') || link;
          let title = titleEl?.textContent?.trim() || '';
          title = title.replace(/\s+/g, ' ').trim();
          if (!title || title.length < 3 || title.length > 200) return;

          const allText = container.textContent || '';

          // Price
          let price = null;
          const priceEls = container.querySelectorAll('[class*="price"], [class*="Price"], [class*="amount"]');
          priceEls.forEach(el => {
            const m = el.textContent.match(/[¥￥]?\s*([\d,]+)/);
            if (m && !price) price = parseFloat(m[1].replace(/,/g, ''));
          });
          if (!price) {
            const pm = allText.match(/(?:CN\s*¥|¥|￥)\s*([\d,]+)/);
            if (pm) price = parseFloat(pm[1].replace(/,/g, ''));
          }

          // Rating
          let rating = null;
          const ratingEls = container.querySelectorAll('[class*="rating"], [class*="Rating"], [class*="score"]');
          ratingEls.forEach(el => {
            const m = el.textContent.match(/([\d.]+)/);
            if (m && !rating) {
              const v = parseFloat(m[1]);
              if (v >= 1 && v <= 5) rating = v;
            }
          });

          // Review count
          let reviewCount = null;
          const rm = allText.match(/\(([\d,]+)\s*\)/) || allText.match(/([\d,]+)\s*(?:条|reviews|评)/i);
          if (rm) reviewCount = parseInt(rm[1].replace(/,/g, ''), 10);

          const imgEl = container.querySelector('img[src*="http"]');
          const durMatch = allText.match(/(\d+\.?\d*)\s*(小时|分钟|hours?|mins?|天|days?)/i);
          const idMatch = href.match(/-t(\d+)/);
          const activityId = idMatch ? idMatch[1] : href.replace(/[^a-zA-Z0-9]/g, '_').slice(-20);

          items.push({
            product_id: `GYG_AUTO_${activityId}`,
            platform: 'GetYourGuide',
            title,
            category: '',
            spu_type: '',
            rating,
            review_count: reviewCount,
            price_cny: price,
            duration: durMatch ? durMatch[0] : '',
            source_url: href.startsWith('http') ? href : `https://www.getyourguide.com${href}`,
            image_url: imgEl?.src || '',
            badge: '',
            page_num: pNum,
          });
        });

        return items;
      }, pageNum);

      onProgress(`GetYourGuide: Page ${pageNum} found ${products.length} products`);

      if (products.length === 0) {
        if (pageNum === 1) {
          onProgress(`GetYourGuide: No products on page 1. Waiting 20s for CAPTCHA...`);
          await delay(20000);
          const retry = await page.evaluate(() => document.querySelectorAll('a[href*="-t"]').length);
          if (retry > 0) { pageNum--; continue; }
          onProgress(`GetYourGuide: Still no products, stopping.`);
        }
        break;
      }

      const result = await onBatchReady(products);
      totalNew += result.written;
      totalDup += result.duplicates;
      onProgress(`GetYourGuide: Page ${pageNum} done - ${result.written} new, ${result.duplicates} dups (Total: ${totalNew} new, ${totalDup} dups)`);

      if (pageNum < maxPages) {
        const waitTime = 3000 + Math.random() * 4000;
        onProgress(`GetYourGuide: Waiting ${(waitTime / 1000).toFixed(1)}s...`);
        await delay(waitTime);
      }
    }
  } finally {
    await browser.close();
  }

  onProgress(`GetYourGuide: Completed! ${totalNew} new records, ${totalDup} duplicates`);
}

module.exports = { run };
