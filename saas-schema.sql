-- Step 1: Create base tables for core entities
create database saas;
use saas;

CREATE TABLE companies (
    id VARCHAR(24) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    revenue VARCHAR(50),
    logo_url VARCHAR(255),
    website_url VARCHAR(255),
    phone_number VARCHAR(50),
    founded_at VARCHAR(4),
    description TEXT,
    is_public BOOLEAN DEFAULT FALSE,
    employee_count INTEGER,
    technologies_total INTEGER,
    news_items_total INTEGER,
    job_posting_totals INTEGER,
    retail_location_count INTEGER,
    employee_contacts_total INTEGER,
    employee_email_addresses_total INTEGER,
    total_funding_amount VARCHAR(50),
    latest_funding_amount VARCHAR(50),
    publicly_traded_symbol VARCHAR(10),
    publicly_traded_exchange VARCHAR(20),
    market_cap VARCHAR(50),
    success BOOLEAN,
    letter VARCHAR(25),
    last_updated datetime
);

-- Step 2: Create location related table
CREATE TABLE company_locations (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    street_address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    postal_code VARCHAR(20),
    UNIQUE(company_id)
);

-- Step 3: Create social links table
CREATE TABLE social_links (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    blog_url VARCHAR(255),
    twitter_url VARCHAR(255),
    youtube_url VARCHAR(255),
    facebook_url VARCHAR(255),
    linkedin_url VARCHAR(255),
    angellist_url VARCHAR(255),
    instagram_url VARCHAR(255),
    crunchbase_url VARCHAR(255),
    UNIQUE(company_id)
);

-- Step 4: Create technologies table
CREATE TABLE technologies (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    name VARCHAR(255),
    category VARCHAR(100)
);

-- Step 5: Create people and employment history tables
CREATE TABLE people (
    id VARCHAR(24) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    company_id VARCHAR(24) REFERENCES companies(id)
);

CREATE TABLE alumni (
    id VARCHAR(24) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    company_id VARCHAR(24) REFERENCES companies(id)
);

CREATE TABLE employment_history (
    id SERIAL PRIMARY KEY,
    person_id VARCHAR(24) REFERENCES people(id),
    company_id VARCHAR(24) REFERENCES companies(id),
    title VARCHAR(255),
    is_current BOOLEAN DEFAULT FALSE,
    start_date datetime,
    end_date datetime,
    organization_id VARCHAR(24)
);

-- Step 6: Create news items table
CREATE TABLE news_items (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    title TEXT,
    url TEXT,
    published_at datetime
);

CREATE TABLE news_event_categ (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    event_category VARCHAR(100)
);
CREATE TABLE news_org_id (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    organization_ids VARCHAR(24)
);
-- Step 7: Create funding rounds table
CREATE TABLE funding_rounds (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    funding_month VARCHAR(50),
    funding_year INTEGER,
    funding_type VARCHAR(50),
    funding_amount BIGINT
);
-- Step 8: Create job postings table
CREATE TABLE job_postings (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    title VARCHAR(255),
    url TEXT,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    posted_at datetime,
    organization_id VARCHAR(24)
);


-- Step 9: Create company keywords and industries tables
CREATE TABLE company_keywords (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    keyword VARCHAR(255)
);

CREATE TABLE company_industries (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    industry VARCHAR(255)
);

-- Add similar companies related table
CREATE TABLE similar_companies (
    id VARCHAR(24) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    revenue VARCHAR(50),
    logo_url VARCHAR(255),
    employee_count INTEGER
);

-- Junction table for company-similar company relationship
CREATE TABLE company_similar_companies (
    company_id VARCHAR(24) REFERENCES companies(id),
    similar_company_id VARCHAR(24) REFERENCES similar_companies(id),
    PRIMARY KEY (company_id, similar_company_id)
);

-- Junction table for similar companies industries
CREATE TABLE similar_company_industries (
    id SERIAL PRIMARY KEY,
    similar_company_id VARCHAR(24) REFERENCES similar_companies(id),
    industry VARCHAR(255)
);

-- Add organization chart related tables
CREATE TABLE org_chart (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    employee_name VARCHAR(255),
    title VARCHAR(255),
    total_reportees INTEGER DEFAULT 0,
    UNIQUE(company_id, employee_name)
);

CREATE TABLE org_chart_reportees (
    id SERIAL PRIMARY KEY,
    org_chart_id INTEGER REFERENCES org_chart(id),
    reportee_name VARCHAR(255),
    reportee_title VARCHAR(255)
);






-- Add new tables for employee metrics
CREATE TABLE employee_metrics (
    id SERIAL PRIMARY KEY,
    company_id VARCHAR(24) REFERENCES companies(id),
    start_date DATE,
    UNIQUE(company_id, start_date)
);

CREATE TABLE department_metrics (
    id SERIAL PRIMARY KEY,
    employee_metric_id INTEGER REFERENCES employee_metrics(id),
    department_function VARCHAR(100),
    new_employees INTEGER DEFAULT 0,
    churned_employees INTEGER DEFAULT 0,
    retained_employees INTEGER DEFAULT 0
);





-- Create indexes for the new tables
CREATE INDEX idx_employee_metrics_date ON employee_metrics(start_date);
CREATE INDEX idx_department_metrics_function ON department_metrics(department_function);
CREATE INDEX idx_org_chart_employee ON org_chart(employee_name);
CREATE INDEX idx_org_chart_reportees_name ON org_chart_reportees(reportee_name);
CREATE INDEX idx_similar_companies_name ON similar_companies(name);
CREATE INDEX idx_similar_companies_revenue ON similar_companies(revenue);
CREATE INDEX idx_similar_companies_employee_count ON similar_companies(employee_count);
-- Step 10: Create indexes for optimization
CREATE INDEX idx_company_name ON companies(name);
CREATE INDEX idx_company_revenue ON companies(revenue);
CREATE INDEX idx_company_employee_count ON companies(employee_count);
CREATE INDEX idx_company_location_city ON company_locations(city);
CREATE INDEX idx_company_location_country ON company_locations(country);
CREATE INDEX idx_people_names ON people(first_name, last_name);
CREATE INDEX idx_employment_dates ON employment_history(start_date, end_date);
CREATE INDEX idx_news_published ON news_items(published_at);
CREATE INDEX idx_job_postings_date ON job_postings(posted_at);
CREATE INDEX idx_technologies_name ON technologies(name);
CREATE INDEX idx_funding_amount ON funding_rounds(funding_amount);




select * from  alumni;
select * from companies;
select * from  company_industries;
select * from   company_keywords;
select * from  company_locations;
select * from   company_similar_companies;
select * from   department_metrics;
select * from   employee_metrics;
 select * from     employment_history;
  select * from    funding_rounds;
 select * from     job_postings;
  select * from    news_event_categ;
 select * from     news_items;
select * from      news_org_id;
 select * from     org_chart;
  select * from    org_chart_reportees;
  select * from    people;
  select * from    similar_companies;
  select * from    similar_company_industries;
  select * from    social_links;
  select * from    technologies;




TRUNCATE TABLE alumni;
TRUNCATE TABLE     companies;
 TRUNCATE TABLE    company_industries;
 TRUNCATE TABLE    company_keywords;
  TRUNCATE TABLE   company_locations;
  TRUNCATE TABLE   company_similar_companies;
 TRUNCATE TABLE    department_metrics;
  TRUNCATE TABLE   employee_metrics;
 TRUNCATE TABLE    employment_history;
  TRUNCATE TABLE   funding_rounds;
 TRUNCATE TABLE    job_postings;
  TRUNCATE TABLE   news_event_categ;
 TRUNCATE TABLE    news_items;
TRUNCATE TABLE     news_org_id;
 TRUNCATE TABLE    org_chart;
  TRUNCATE TABLE   org_chart_reportees;
  TRUNCATE TABLE   people;
  TRUNCATE TABLE   similar_companies;
  TRUNCATE TABLE   similar_company_industries;
  TRUNCATE TABLE   social_links;
  TRUNCATE TABLE   technologies;



show tables;
drop table employee_metrics;
drop database saas;