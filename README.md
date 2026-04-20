# Indeed Scraper

Simple Indeed scraper that uses Crawl4AI, ScrapeOps, and a persistent browser profile for optional Google sign-in.

## Setup

1. Install Python 3.11.
2. Create a virtual environment.
3. Install dependencies:

```bash
pip install -r requirements.txt
playwright install --with-deps chromium
```

4. Copy `.env.example` to `.env` and set:

```env
SCRAPEOPS_API_KEY=your_key
INDEED_USER_DATA_DIR=.browser_profile
INDEED_HEADLESS=true
```

5. If you need to sign in through Google for the first time, set `INDEED_HEADLESS=false`, run the scraper once, log in in the browser, then switch it back to `true`.

## Run

```bash
python main.py --keyword "Data Scientist" --location "Bangalore"
```

Optional:

```bash
python main.py --keyword "Data Scientist" --location "Bangalore" --max-pages 10
```

## Output

The scraper writes:
- `data/jobs/YYYY-MM-DD/<keyword>_<location>.csv`
- `data/manifests/manifest.json`

## Notes

- The CSV is intentionally compact and easier to review than a nested JSON dump.
- The manifest is used only for deduplication.
- If login is needed, the browser profile folder stores the session locally on the machine where you ran the login.
