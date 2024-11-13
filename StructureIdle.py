import json
from datetime import datetime

def toStructureData(data):
    def structure_data(item):
        try:
            # Only parse if it's a string and looks like JSON
            if isinstance(item.get('data'), str):
                try:
                    item['data'] = json.loads(item['data'])
                except json.JSONDecodeError:
                    # If parsing fails, keep original data
                    pass
            return item
        except Exception as e:
            print(f"Error processing item: {e}")
            return item

    def sort_data(data, key):
        try:
            return sorted(data, key=lambda x: x.get(key, ""))
        except Exception as e:
            print(f"Error sorting data: {e}")
            return data

    # Structure the data
    structured_data = [structure_data(item) for item in data]
    # Sort the data
    sorted_structured_data = sort_data(structured_data, 'name')
    
    print("Data structured successfully......")
    return sorted_structured_data

def amountChange(value):
    if not value or not isinstance(value, str):
        return None
        
    suffix_map = {
        'T': 1_000,        # Thousands
        'K': 1_000,        # Thousands (alternative for T)
        'L': 100_000,      # Lakhs (100,000)
        'M': 1_000_000,    # Millions
        'B': 1_000_000_000 # Billions
    }
    
    value = value.strip().upper()
    
    try:
        if value[-1] in suffix_map:
            numeric_part = float(value[:-1])
            suffix = value[-1]
            return int(numeric_part * suffix_map[suffix])
        else:
            return int(float(value))
    except (ValueError, IndexError):
        return None

def dateChange(value):
    if not value:
        return None
    
    else:
        x= value[0:10]+' '+value[11:19]
        return x

def process_data(data):
    amountToBeChanged = ['revenue', 'latest_funding_amount', 'total_funding_amount', 'market_cap']
    dateToBeChanged = ['last_updated']
    
    # First ensure data is structured
    if isinstance(data, dict):
        data = [data]
    
    processed_data = []
    for item in data:
        # Create a copy of the item to avoid modifying the original
        processed_item = dict(item)
        
        if 'data' in processed_item and isinstance(processed_item['data'], dict):
            data_dict = processed_item['data']
            for key, value in data_dict.items():
                if key in amountToBeChanged:
                    data_dict[key] = amountChange(value)
                elif key in dateToBeChanged:
                    data_dict[key] = dateChange(value)
        
        processed_data.append(processed_item)
    
    return processed_data

def convert_dates(data):
    for company in data:
        company_data = company.get("data", {})
        people = company_data.get("people", [])
        alumni = company_data.get("alumni", [])
        job_posting_urls = company_data.get("job_posting_urls", [])
        news = company_data.get("news", [])
        similar_companies = company_data.get("similar_companies", [])
        
        
        for field in similar_companies:
            for key,val in field.items():
                if( key =="revenue"):
                    field[key]=amountChange(val)
                
        # print(job_posting_urls)

        for field in job_posting_urls:
            for x,y in field.items():
                if x=="posted_at":
                    field[x]=dateChange(y)
                    

        # Iterate through each technology in the list of technologies
        for field in news:
            for i,j in field.items():
                if i=='published_at':
                    field[i] = dateChange(j)
                
        # Process both 'people' and 'alumni' fields
        for person_list in [people, alumni]:
            for person in person_list:
                employment_history = person.get("employment_history", [])
                for job in employment_history:
                    # Modify 'start_date' if it exists
                    if "start_date" in job and isinstance(job["start_date"], str):
                        job["start_date"] = dateChange(job["start_date"])
                        # print(job["start_date"])
                    # Modify 'end_date' if it exists
                    if "end_date" in job and isinstance(job["end_date"], str):
                        job["end_date"] = dateChange(job["end_date"])
                        # print(job["end_date"])

    return data


def main(inputFile, outputFile):
    try:
        # Load and process the data
        with open(inputFile, 'r', encoding='utf-8') as file:
            raw_data = json.load(file)
            print("File loaded successfully.")
        
        # Structure the data first
        structured_data = toStructureData(raw_data)
        
        raw_data2=convert_dates(structured_data)
        
        structured_data2 = toStructureData(raw_data2)
        # Process the structured data
        processed_data = process_data(structured_data2)
        
        # Save the processed data
        with open(outputFile, 'w', encoding='utf-8') as file:
            json.dump(processed_data, file, default=str, indent=2)
            print("Processed data saved successfully.")
            
    except FileNotFoundError:
        print("Error: JSON file not found.")
    except json.JSONDecodeError:
        print("Error: Invalid JSON format.")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

# Execute the program
inputFile = 'D:\\work_place\\python\\data_example.json'
outputFile = 'D:\\work_place\\python\\Key101.json'
main(inputFile, outputFile)