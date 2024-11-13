# Import necessary libraries
import json
import mysql.connector
from mysql.connector import Error

# Define the path to the input file
inputFile = 'D:\\work_place\\python\\Key101.json'

def fetch_from_db(query, values=None):
    """
    Executes a SELECT query and returns the result.
    
    Parameters:
    - query (str): The SQL query to execute.
    - values (tuple): The values to substitute in the query placeholders.
    
    Returns:
    - The result of the query (either a single row or a list of rows).
    """
    try:
        # Assuming you have a cursor object already set up
        cursor.execute(query, values)
        result = cursor.fetchone()  # Fetch the first row of the result
        
        # Uncomment the following line if you want all rows for a multi-row result
        # result = cursor.fetchall()
        
        return result
    except Error as err:
        print(f"Error executing query: {err}")
        return None


# Function to execute insert operations
def insert_into_db(query, values):
    try:
        cursor.execute(query, values)
        db.commit()
        return cursor.lastrowid
    except Error as e:
        print(f"Error executing query: {e}")
        db.rollback()
        return None

# Function to insert company data into the database
def insert_company(data):
    fields = [
        'id', 'name', 'revenue', 'logo_url', 'is_public', 'founded_at',
        'market_cap', 'description', 'website_url', 'last_updated', 
        'phone_number', 'employee_count', 'news_items_total',
        'job_posting_totals', 'technologies_total', 'total_funding_amount', 
        'latest_funding_amount', 'retail_location_count', 'publicly_traded_symbol',
        'employee_contacts_total', 'publicly_traded_exchange', 'employee_email_addresses_total'
    ]
    
    
    for idx in data:
        company_data = {key: value for key, value in idx['data'].items() if key in fields}
        success=idx.get("success",False)
        letter=idx.get("letter",None)
        employment_query = """
            INSERT INTO companies (
                id, name, revenue, logo_url, website_url, phone_number, founded_at, description, is_public, employee_count,
                technologies_total, news_items_total, job_posting_totals, retail_location_count, employee_contacts_total,
                employee_email_addresses_total, total_funding_amount, latest_funding_amount, publicly_traded_symbol,
                publicly_traded_exchange, market_cap,success,letter, last_updated
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s)
        """
        values = (
            company_data.get("id"), company_data.get("name"), company_data.get("revenue"), company_data.get("logo_url"),
            company_data.get("website_url"), company_data.get("phone_number"), company_data.get("founded_at"),
            company_data.get("description"), company_data.get("is_public"), company_data.get("employee_count"),
            company_data.get("technologies_total"), company_data.get("news_items_total"), company_data.get("job_posting_totals"),
            company_data.get("retail_location_count"), company_data.get("employee_contacts_total"), company_data.get("employee_email_addresses_total"),
            company_data.get("total_funding_amount"), company_data.get("latest_funding_amount"),
            company_data.get("publicly_traded_symbol"), company_data.get("publicly_traded_exchange"),
            company_data.get("market_cap"),success,letter, company_data.get("last_updated")
        )   

        insert_into_db(employment_query, values)

# Function to insert location data into the database
def insert_location(data):
    for company in data:
        company_id = company.get("id")
        if not company_id:
            continue
        # Use company ID as foreign key
        company_data = company.get("data", {})
        location = company_data.get("location", {})

        # Extract location fields with default values
        street_address = location.get("street_address", None)
        city = location.get("city", None)
        state = location.get("state", None)
        country = location.get("country", None)
        postal_code = location.get("postal_code", None)

        # Create SQL insert statement for locations
        location_query = """
            INSERT INTO company_locations (company_id, street_address, city, state, country, postal_code)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                street_address=VALUES(street_address), city=VALUES(city),
                state=VALUES(state), country=VALUES(country), postal_code=VALUES(postal_code);
        """
        location_values = (company_id, street_address, city, state, country, postal_code)
        insert_into_db(location_query, location_values)


# Function to insert social link data into the database
def insert_social_links(data):
    for company in data:
        company_id = company.get("id") 
        if not company_id:
            continue# Use company ID as foreign key
        company_data = company.get("data", {})
        social_links = company_data.get("social_links", {})

        # Extract social link fields with default values
        blog_url = social_links.get("blog_url", None)
        twitter_url = social_links.get("twitter_url", None)
        youtube_url = social_links.get("youtube_url", None)
        facebook_url = social_links.get("facebook_url", None)
        linkedin_url = social_links.get("linkedin_url", None)
        angellist_url = social_links.get("angellist_url", None)
        instagram_url = social_links.get("instagram_url", None)
        crunchbase_url = social_links.get("crunchbase_url", None)

        # Create SQL insert statement for social_links
        social_links_query = """
            INSERT INTO social_links (
                company_id, blog_url, twitter_url, youtube_url, facebook_url,
                linkedin_url, angellist_url, instagram_url, crunchbase_url
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                blog_url=VALUES(blog_url), twitter_url=VALUES(twitter_url),
                youtube_url=VALUES(youtube_url), facebook_url=VALUES(facebook_url),
                linkedin_url=VALUES(linkedin_url), angellist_url=VALUES(angellist_url),
                instagram_url=VALUES(instagram_url), crunchbase_url=VALUES(crunchbase_url);
        """
        social_links_values = (
            company_id, blog_url, twitter_url, youtube_url, facebook_url,
            linkedin_url, angellist_url, instagram_url, crunchbase_url
        )

        # Execute the query to insert or update social links
        insert_into_db(social_links_query, social_links_values)

# Function to insert technology data into the database
def insert_technologies(data):
    for company in data:
        company_id = company.get("id") 
        if not company_id:
            continue# Use company ID as foreign key
        company_data = company.get("data", {})
        technologies = company_data.get("technologies", [])

        # Iterate through each technology in the list of technologies
        for tech in technologies:
            name = tech.get("name", None)
            category = tech.get("category", None)

            # Create SQL insert statement for technologies
            technology_query = """
                INSERT INTO technologies (company_id, name, category)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    name=VALUES(name), category=VALUES(category);
            """
            technology_values = (company_id, name, category)
            insert_into_db(technology_query, technology_values)
      
def insert_alumni(data):
    for company in data:
        company_id = company.get("id")  # Use company ID as foreign key
        if not company_id:
            #print("Skipping people entry with missing company ID")
            continue
        company_data = company.get("data", {})
        alumni = company_data.get("alumni", [])

        # Iterate through each person in the list of people
        for person in alumni:
            if(person.get("id")==None):
                continue
            person_id = person.get("id")
            first_name = person.get("first_name")
            last_name = person.get("last_name")

            # Create SQL insert statement for people
            people_query = """
                INSERT INTO alumni (id, first_name, last_name, company_id)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    first_name=VALUES(first_name), last_name=VALUES(last_name), company_id=VALUES(company_id);
            """
            people_values = (person_id, first_name, last_name, company_id)
            insert_into_db(people_query, people_values)
            
def insert_people(data):
    for company in data:
        company_id = company.get("id")  # Use company ID as foreign key
        if not company_id:
            #print("Skipping people entry with missing company ID")
            continue
        company_data = company.get("data", {})
        people = company_data.get("people", [])

        # Iterate through each person in the list of people
        for person in people:
            if(person.get("id")==None):
                continue
            person_id = person.get("id")
            first_name = person.get("first_name")
            last_name = person.get("last_name")

            # Create SQL insert statement for people
            people_query = """
                INSERT INTO people (id, first_name, last_name, company_id)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    first_name=VALUES(first_name), last_name=VALUES(last_name), company_id=VALUES(company_id);
            """
            people_values = (person_id, first_name, last_name, company_id)
            insert_into_db(people_query, people_values)

def insert_employment_history(data):
    for company in data:
        company_id = company.get("id")
        if not company_id:
            continue  # Use company ID as foreign key
        company_data = company.get("data", {})

        # Process both 'people' and 'alumni' fields
        for source_type in ["people", "alumni"]:
            person_list = company_data.get(source_type, [])

            # Iterate through each person in the list
            for person in person_list:
                person_id = person.get("id")
                if not person_id:
                    continue  # Skip if person ID is missing
                
                # Insert employment history for each person
                employment_history = person.get("employment_history", [])
                for job in employment_history:
                    title = job.get("title")
                    is_current = job.get("is_current", False)
                    start_date = job.get("start_date")
                    end_date = job.get("end_date")
                    organization_id = job.get("organization_id")

                    # Create SQL insert statement without 'source_type'
                    employment_query = """
                        INSERT INTO employment_history (
                            person_id, company_id, title, is_current, start_date, end_date, organization_id
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON DUPLICATE KEY UPDATE
                            title=VALUES(title), is_current=VALUES(is_current),
                            start_date=VALUES(start_date), end_date=VALUES(end_date),
                            organization_id=VALUES(organization_id);
                    """
                    employment_values = (
                        person_id, company_id, title, is_current, start_date, end_date, organization_id
                    )
                    insert_into_db(employment_query, employment_values)

        

# Function to insert news items into the database
def insert_news_items(data):
    for company in data:
        company_id = company.get("id")
        if not company_id:
            continue  # Skip if company ID is missing

        company_data = company.get("data", {})
        news = company_data.get("news", [])
        
        # Insert data for each news item
        for field in news:
            # Extract news item fields with default values
            title = field.get("title", None)
            url = field.get("url", None)
            published_at = field.get("published_at", None)
            
            # Insert into news_items table
            news_item_query = """
                INSERT INTO news_items (company_id, title, url, published_at)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title=VALUES(title), url=VALUES(url), published_at=VALUES(published_at);
            """
            news_item_values = (company_id, title, url, published_at)
            insert_into_db(news_item_query, news_item_values)
            
            # Insert each event category from event_categories list into news_event_categ table
            event_categories = field.get("event_categories", [])
            for event_category in event_categories:
                event_category_query = """
                    INSERT INTO news_event_categ (company_id, event_category)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        event_category=VALUES(event_category);
                """
                event_category_values = (company_id, event_category)
                insert_into_db(event_category_query, event_category_values)
            
            # Insert each organization ID from organization_ids list into news_org_id table
            organization_ids = field.get("organization_ids", [])
            for organization_id in organization_ids:
                organization_id_query = """
                    INSERT INTO news_org_id (company_id, organization_ids)
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE
                        organization_ids=VALUES(organization_ids);
                """
                organization_id_values = (company_id, organization_id)
                insert_into_db(organization_id_query, organization_id_values)



def insert_funding_rounds(data):
    for company in data:
        company_id = company.get("id")
        if not company_id:
            continue  # Skip if company ID is missing

        company_data = company.get("data", {})
        funding_rounds = company_data.get("funding_rounds", [])
        
        year=2000
        for field in funding_rounds:
            
            for x,y in field.items():
                if x=='date':
                    year=field[x][-4:]
                    field[x]=field[x][:-5]
                  
            funding_month=field.get("date",None)
            funding_type=field.get("type",None)
            funding_amount=field.get("funding_amount",None)
            
            # Create SQL insert statement for job postings
            query = """ 
                INSERT INTO  funding_rounds(
                    company_id,funding_month,funding_year,funding_type,funding_amount
                )
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    funding_month=VALUES(funding_month), funding_year=VALUES(funding_year), funding_amount=VALUES(funding_amount),
                    funding_type=VALUES(funding_type);
            """
            values = (company_id,funding_month,year,funding_type,funding_amount)
            insert_into_db(query, values)


def insert_job_postings(data):
    for company in data:
        company_id = company.get("id")
        if not company_id:
            continue  # Skip if company ID is missing

        company_data = company.get("data", {})
        job_posting_urls = company_data.get("job_posting_urls", [])
        
        # Iterate through each job posting entry in job_posting_urls
        for posting in job_posting_urls:
            # Extract fields for each job posting
            url = posting.get("url", None)
            city = posting.get("city", None)
            state = posting.get("state", None)
            title = posting.get("title", None)
            country = posting.get("country", None)
            posted_at = posting.get("posted_at", None)
            organization_id = posting.get("organization_id", None)
            
            # Create SQL insert statement for job postings
            query = """ 
                INSERT INTO job_postings (
                    company_id,url,city,state,title,country,posted_at,
                    organization_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    title=VALUES(title), organization_id=VALUES(organization_id), 
                    url=VALUES(url), state=VALUES(state), country=VALUES(country),
                    posted_at=VALUES(posted_at);
            """
            values = (company_id, url, city, state, title, country, posted_at, organization_id)
            insert_into_db(query, values)
        
def insert_company_keywords(data):
    for company in data:
        company_id = company.get("id")
        if not company_id:
            continue  # Skip if company ID is missing

        company_data = company.get("data", {})
        keywords = company_data.get("keywords", [])

        #keyword
        for x in keywords:
            query = """ 
                INSERT INTO  company_keywords(
                    company_id,keyword
                    )
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE 
                    keyword=VALUES(keyword);
            """
            values = (company_id,x)
            insert_into_db(query, values)

#//////////////////////////////////////////////////////////////////////////////////////



def insert_company_industries(data):
    for company in data:
        company_id = company.get("id")
        if not company_id:
            continue  # Skip if company ID is missing
        cd=company.get("data")
        industries = cd.get("industries", [])

        # Iterate through each industry associated with the company
        for industry in industries:
            query = """ 
                INSERT INTO company_industries (
                    company_id, industry
                )
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE 
                    industry=VALUES(industry);
            """
            values = (company_id, industry)
            insert_into_db(query, values)
   
def insert_similar_companies(data):
    for company in data:
        company_data = company.get("data", {})
        similar_companies = company_data.get("similar_companies", [])

        for field in similar_companies:
            id = field.get("id")  # No trailing commas
            name = field.get("name")
            revenue = field.get("revenue")
            logo_url = field.get("logo_url")
            employee_count = field.get("employee_count")

            query = """ 
                INSERT INTO similar_companies (
                    id, name, revenue, logo_url, employee_count
                )
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    name=VALUES(name), revenue=VALUES(revenue), logo_url=VALUES(logo_url), employee_count=VALUES(employee_count);
            """
            values = (id, name, revenue, logo_url, employee_count)
            insert_into_db(query, values)
            
            
def insert_company_similar_companies(data):
    """
    Inserts data into the company_similar_companies junction table.
    """
    for company in data:
        company_id = company.get("id")  # ID of the main company
        company_data = company.get("data", {})
        similar_companies = company_data.get("similar_companies", [])

        # Validate structure
        if not company_id or not isinstance(similar_companies, list):
            print(f"Skipping invalid company or similar companies data: {company}")
            continue

        for similar_company in similar_companies:
            similar_company_id = similar_company.get("id")
            if not similar_company_id:
                print(f"Skipping invalid similar company data: {similar_company}")
                continue

            query = """
                INSERT INTO company_similar_companies (
                    company_id, similar_company_id
                )
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE company_id=VALUES(company_id), similar_company_id=VALUES(similar_company_id);
            """
            values = (company_id, similar_company_id)
            insert_into_db(query, values)

def insert_similar_company_industries(data):
    """
    Inserts data into the similar_company_industries junction table.
    """
    for company in data:
        company_data = company.get("data", {})
        similar_companies = company_data.get("similar_companies", [])

        # Validate structure
        if not isinstance(similar_companies, list):
            print(f"Skipping invalid similar companies data: {company_data}")
            continue

        for similar_company in similar_companies:
            similar_company_id = similar_company.get("id")
            industries = similar_company.get("industries", [])

            if not similar_company_id or not isinstance(industries, list):
                print(f"Skipping invalid similar company or industries data: {similar_company}")
                continue

            for industry in industries:
                query = """
                    INSERT INTO similar_company_industries (
                        similar_company_id, industry
                    )
                    VALUES (%s, %s)
                    ON DUPLICATE KEY UPDATE industry=VALUES(industry);
                """
                values = (similar_company_id, industry)
                insert_into_db(query, values)
                
                
def insert_org_chart(data):
    """
    Inserts data into the org_chart table.
    """
    for company in data:
        company_id = company.get("id")  # ID of the main company
        company_data = company.get("data", {})
        org_chart_data = company_data.get("org_chart_data")

        # Validate structure
        if not company_id or org_chart_data is None:  # Add check for org_chart_data
            continue

        employee_name = org_chart_data.get("name")
        title = org_chart_data.get("title", None)
        total_reportees = org_chart_data.get("total_reportees", 0)

        if not employee_name:
            continue
        
        query = """
            INSERT INTO org_chart (
                company_id, employee_name, title, total_reportees
            )
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                title=VALUES(title), total_reportees=VALUES(total_reportees);
        """
        values = (company_id, employee_name, title, total_reportees)
        insert_into_db(query, values)


def insert_org_chart_reportees(data):
    """
    Inserts data into the org_chart_reportees table.
    """
    for company in data:
        company_data = company.get("data", {})
        org_chart_data = company_data.get("org_chart_data")

        # Ensure org_chart_data is not None
        if org_chart_data is None:
            continue

        employee_name = org_chart_data.get("name")
        reportees = org_chart_data.get("reportees", [])  # List of reportees

        # Fetch the org_chart_id for the current employee_name and company_id
        query_select = """
            SELECT id FROM org_chart 
            WHERE company_id = %s AND employee_name = %s;
        """
        values_select = (company.get("id"), employee_name)
        cursor.execute(query_select, values_select)
        org_chart_id = cursor.fetchone()

        if not org_chart_id:
            print(f"Org chart entry not found for {employee_name} in company {company.get('id')}.")
            continue

        for reportee in reportees:
            reportee_name = reportee.get("name")
            reportee_title = reportee.get("title")

            if not reportee_name or not reportee_title:
                continue

            query_insert = """
                INSERT INTO org_chart_reportees (
                    org_chart_id, reportee_name, reportee_title
                )
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE 
                    reportee_title=VALUES(reportee_title);
            """
            values_insert = (org_chart_id[0], reportee_name, reportee_title)
            insert_into_db(query_insert, values_insert)


def insert_company_metrics(data):
    """
    Inserts data into the employee_metrics table.
    """
    for company in data:
        company_id = company.get("id")  # ID of the main company
        company_data = company.get("data", {})
        employee_metrics = company_data.get("employee_metrics", [])

        # Validate structure
        if not company_id or not isinstance(employee_metrics, list):
            # Skip if company_id is missing or employee_metrics is not a list
            continue
        
        for metric in employee_metrics:
            start_date = metric.get("start_date")
            if not start_date:
                # Skip if start_date is missing
                continue

            # Insert into employee_metrics table
            query = """
                INSERT INTO employee_metrics (company_id, start_date)
                VALUES (%s, %s)
                ON DUPLICATE KEY UPDATE company_id = company_id;
            """
            values = (company_id, start_date)

            # Execute query to insert or update the record
            employee_metric_id = insert_into_db(query, values)

            # Retrieve the ID of the inserted/updated row
            if employee_metric_id is None:
                # Query the ID if there was a duplicate
                select_query = """
                    SELECT id FROM employee_metrics
                    WHERE company_id = %s AND start_date = %s;
                """
                employee_metric_id = fetch_from_db(select_query, values)

            if employee_metric_id and "departments" in metric:
                insert_department_metrics(employee_metric_id, metric["departments"])


def insert_department_metrics(employee_metric_id, departments):
    """
    Inserts data into the department_metrics table.
    """
    for department in departments:
        department_function = department.get("functions")
        new_employees = department.get("new", 0)
        churned_employees = department.get("churned", 0)
        retained_employees = department.get("retained", 0)

        query = """
            INSERT INTO department_metrics (
                employee_metric_id, department_function, new_employees, churned_employees, retained_employees
            )
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                new_employees = VALUES(new_employees),
                churned_employees = VALUES(churned_employees),
                retained_employees = VALUES(retained_employees);
        """
        values = (
            employee_metric_id,
            department_function,
            new_employees,
            churned_employees,
            retained_employees,
        )
        insert_into_db(query, values)




try:
    db = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Yashc6h6.sql",
        database="saas"
    )
    if db.is_connected():
        print("Connected to MySQL Database")
    cursor = db.cursor()
    
    # Load the JSON data
    with open(inputFile, 'r', encoding='utf-8') as f:
        data = json.load(f)
        print("File loaded successfully")
        
    insert_company(data)
    insert_location(data)
    insert_social_links(data)
    insert_technologies(data)
    insert_alumni(data)
    insert_people(data)
    insert_employment_history(data)
    insert_news_items(data)
    insert_funding_rounds(data),
    insert_job_postings(data),
    insert_company_keywords(data),
    insert_company_industries (data)
    insert_similar_companies(data)
    insert_company_similar_companies(data)
    insert_similar_company_industries(data)
    insert_org_chart(data)
    insert_org_chart_reportees(data)
    insert_company_metrics(data)
    
# Additional tables can be processed similarly with different functions (e.g., insert_company_locations, insert_social_links)

except Error as err:
    print(f"Error while connecting to MySQL: {err}")
finally:
    if db.is_connected():
        cursor.close()
        db.close()
        print("Database connection closed")
