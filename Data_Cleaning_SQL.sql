-- Data Cleaning and Standardization

-- Create a staging table
CREATE TABLE layoffs_staging LIKE layoffs;

-- Copy data to the staging table
INSERT INTO layoffs_staging
SELECT * FROM layoffs;

-- Remove duplicates based on key columns
WITH duplicate_cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ORDER BY company) AS row_num
    FROM layoffs_staging
)
DELETE FROM duplicate_cte
WHERE row_num > 1;

-- Standardize text fields
UPDATE layoffs_staging
SET company = TRIM(company);

-- Update industry names
UPDATE layoffs_staging
SET industry = 'Crypto'
WHERE industry LIKE 'crypto%';

-- Standardize country names
UPDATE layoffs_staging
SET country = 'United States'
WHERE country LIKE 'united states%';

-- Remove trailing periods from country names
UPDATE layoffs_staging
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'united states%';

-- Convert date format to proper date type
UPDATE layoffs_staging
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_staging
MODIFY COLUMN `date` DATE;

-- Handle null and blank values
UPDATE layoffs_staging AS t1
JOIN layoffs_staging AS t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '') 
  AND t2.industry IS NOT NULL;

-- Remove unnecessary columns and rows
ALTER TABLE layoffs_staging
DROP COLUMN dup_count;

DELETE FROM layoffs_staging
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

-- Data Analysis

-- Aggregate total layoffs by company
SELECT company, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY company
ORDER BY total_laid_off DESC;

-- Aggregate total layoffs by month
SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY `month`
ORDER BY `month` ASC;

-- Rolling sum of total layoffs by month
WITH rolling_total AS (
    SELECT SUBSTRING(`date`, 1, 7) AS `month`, SUM(total_laid_off) AS total_off
    FROM layoffs_staging
    WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
    GROUP BY `month`
    ORDER BY `month` ASC
)
SELECT `month`, total_off, SUM(total_off) OVER (ORDER BY `month`) AS rolling_total
FROM rolling_total;

-- Aggregate total layoffs by company and year
SELECT company, YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging
GROUP BY company, YEAR(date)
ORDER BY total_laid_off DESC;

-- Rank companies by total layoffs for each year
WITH company_year AS (
    SELECT company, YEAR(date) AS year, SUM(total_laid_off) AS total_laid_off
    FROM layoffs_staging
    GROUP BY company, YEAR(date)
),
company_rank_year AS (
    SELECT *, 
           DENSE_RANK() OVER (PARTITION BY year ORDER BY total_laid_off DESC) AS ranking
    FROM company_year
)
SELECT *
FROM company_rank_year
WHERE ranking <= 5;

-- View data for a specific company
SELECT *
FROM layoffs_staging
WHERE company = 'Google';
