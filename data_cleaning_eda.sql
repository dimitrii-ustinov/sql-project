-- Data cleaning
USE world_layoffs;

select *
from world_layoffs.layoffs as lf;

-- process
# 1. Remove duplicates
# 2. Standardize the data
# 3. Null and/or blank values
# 4. Remove any columns if necessary

-- Start with making a copy of tha table to keep the raw data untouched.
-- CREATE TABLE world_layoffs.layoffs_staging
-- LIKE world_layoffs.layoffs;

-- INSERT layoffs_staging
-- SELECT *
-- FROM layoffs;

select *
from layoffs_staging;


-- 1. Remove Duplicates

# First let's check for duplicates

WITH duplicate_cte as 
(
SELECT *, 
row_number() OVER(
PARTITION BY company, 
location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num = 2;

-- WITH duplicate_cte as 

INSERT INTO layoffs_staging_2
SELECT *
FROM (
SELECT *, 
row_number() OVER(
PARTITION BY company, 
			location, 
            industry, 
            total_laid_off, 
            percentage_laid_off, 
            `date`,
            stage,
            country, 
            funds_raised_millions
            ORDER BY (SELECT NULL)
            ) as row_num
FROM layoffs_staging
) as cte
WHERE row_num = 1;

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_staging2
SELECT *, 
row_number() OVER(
PARTITION BY company, 
location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM layoffs_staging;

SELECT COUNT(company)
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT COUNT(company)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing data
SELECT *
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

# Check industry column and standardize them
SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT COUNT(country)
FROM layoffs_staging2;

# Standardize the countries
SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- United States has a dot at the end.
SELECT DISTINCT country, TRIM(trailing '.' from country)
FROM layoffs_staging2
ORDER BY 1;

# Replace with the trimmed column
UPDATE layoffs_staging2
SET country = TRIM(trailing '.' from country)
WHERE country LIKE 'United%';

SELECT DISTINCT country, TRIM(trailing '.' from country)
FROM layoffs_staging2
ORDER BY 1;

-- Format the date from text to date format

# First adjust the date to the standard format
SELECT *, count(`date` = NULL)
FROM layoffs_staging2;

# Check that there are no null values
SELECT count(`date` = NULL)
FROM layoffs_staging2;

# Check the code
SELECT `date`, str_to_date(date, '%m/%d/%Y')
FROM layoffs_staging2;

# Update the column
UPDATE layoffs_staging2
SET `date` = str_to_date(date, '%m/%d/%Y');

# Check the changes
SELECT *
FROM layoffs_staging2;

# Change the column type
ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

# Check the changes
SELECT *
FROM layoffs_staging2;

-- Fill in the industry for companies that have several entries
# First, identify the empty and null values in industry
SELECT company, industry
FROM layoffs_staging2
WHERE industry IS NULL or industry = '';

# First let's populate empty values with NULL values
UPDATE layoffs_staging2
SET industry = null
WHERE industry = '';

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL or industry = '';

# Next we want to join the table to itself to see if there are similar values for the companies with missing industries.
SELECT *
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL and t2.industry IS NOT NULL;

#Let's update the table since we have 4 rows that have missing values
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

# Check
SELECT *
FROM layoffs_staging2 t1
WHERE t1.industry IS NULL;

# Since we don't plan to use the rows where total_laid_off and percentage_laid_off are empty, we can drop those columns.

#Identify
SELECT * #COUNT(company) = 361
FROM layoffs_staging2
WHERE total_laid_off IS NULL AND percentage_laid_off is NULL;

# Drop
#DELETE FROM layoffs_staging2
#WHERE total_laid_off IS NULL AND percentage_laid_off is NULL;

DELETE FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

# Check
SELECT COUNT(*)
FROM layoffs_staging2;


-- EDA
USE world_layoffs;
# First, we can look into the basic max for total_laid_off to feel the scope of the data
SELECT *
FROM layoffs_staging2;

SELECT company, MAX(total_laid_off) as max_laid
FROM layoffs_staging2
GROUP BY company
HAVING max_laid IS NOT NULL
ORDER BY 2 DESC;

# It looks like the big tech had most laid-offs
# Now let's see if the structure is the same over all the years aggregated

WITH laid_by_company AS
(
SELECT company, SUM(total_laid_off) as max_laid
FROM layoffs_staging2
GROUP BY company 
HAVING SUM(total_laid_off) IS NOT NULL

)
SELECT company, max_laid
FROM laid_by_company
ORDER BY max_laid DESC;
# Still the big tech, but the structure of laid_offs has changed with Amazon leading the list

# Now we can see the structure of laid_off by industry, year and stage of financing
SELECT *
FROM layoffs_staging2;

#First, let's drop the row_num column that is redundant from our cleaning phase
ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

#Next we can see the structure of lay_offs by industry, year, country and stage
#Industry
SELECT industry, SUM(total_laid_off) as laid_off
FROM layoffs_staging2
GROUP BY industry
ORDER BY laid_off DESC;

#By year
SELECT YEAR(`date`) as `year`, SUM(total_laid_off) as laid_off
FROM layoffs_staging2
GROUP BY `year`
HAVING `year` IS NOT NULL
ORDER BY laid_off DESC;

#By country
SELECT country, SUM(total_laid_off) as laid_off
FROM layoffs_staging2
GROUP BY country
ORDER BY laid_off DESC;

#By stage of financing
SELECT stage, SUM(total_laid_off) as laid_off
FROM layoffs_staging2
GROUP BY stage
HAVING stage IS NOT NULL
ORDER BY laid_off DESC;

# Clearly big corporations have more stuff to let go. 
# However, series C and D are the stages when a lot of companies start to choke and it can be interesting to investigate further

#Next we want to understand if there any seasonal patterns in the lay-offs. 
SELECT SUBSTRING(`date`,6,2) as `month`, SUM(total_laid_off) as laid_off
FROM layoffs_staging2
GROUP BY `month`
HAVING `month` IS NOT NULL
ORDER BY `month` ASC;
# Looks like August and September are generally less prone to have laid-offs, 
# while months before and after Christmas have more people being fired.

#Next we want to see ranking and rolling total.

WITH company_year AS
(
SELECT company, YEAR(`date`) as `years`, SUM(total_laid_off) as laid_off
FROM layoffs_staging2
GROUP BY company, `years`
HAVING laid_off IS NOT NULL
#ORDER BY company ASC;
)
, Company_Year_Rank AS 
(
  SELECT company, years, laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY laid_off DESC) AS ranking
  FROM Company_Year
  WHERE years IS NOT NULL
)
SELECT company, years, laid_off, ranking
FROM Company_Year_Rank
#GROUP BY years, company
HAVING ranking <= 5
ORDER BY years, laid_off DESC;

WITH year_month_cte AS
(
SELECT YEAR(`date`) as `years`, SUBSTRING(`date`,6,2) as `months`, SUM(total_laid_off) as laid_off
FROM layoffs_staging2
GROUP BY years, months
HAVING laid_off IS NOT NULL and years IS NOT NULL
ORDER BY years, months
)
SELECT years, months, SUM(laid_off) OVER(ORDER BY years, months) AS rol_num
FROM year_month_cte;
