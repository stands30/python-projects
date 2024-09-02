from fastapi import FastAPI, HTTPException
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError
import csv
from tqdm import tqdm

app = FastAPI()

# Elasticsearch connection setup
ELASTICSEARCH_HOST = "https://localhost:9204"
ELASTICSEARCH_USERNAME = "stanley_dsouza"
ELASTICSEARCH_PASSWORD = "8zsAABimFpW3n5z"

es = Elasticsearch(
    ELASTICSEARCH_HOST,
    http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
    verify_certs=False  # Set this to True if you have a valid SSL certificate
)

# Health check route to verify connection
@app.get("/es_health")
async def es_health():
    try:
        if es.ping():
            return {"status": "Elasticsearch is connected"}
        else:
            raise HTTPException(status_code=500, detail="Unable to connect to Elasticsearch")
    except ConnectionError:
        raise HTTPException(status_code=500, detail="Connection error")

# Example route to get an index
@app.get("/get_index/{index_name}")
async def get_index(index_name: str):
    try:
        if es.indices.exists(index=index_name):
            return es.indices.get(index=index_name)
        else:
            raise HTTPException(status_code=404, detail="Index not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/gen-url")
async def gen_url():
    try:
        response = es.search(
            index='us_states',
            # body={
            #     "query": {
            #         "match": {
            #             "content": query  # Assuming the field you're searching is 'content'
            #         }
            #     }
            # },
            # size=1
        )
        custom_links = es.search(
                index='custom_links',
                # body={
                #     "query": {
                #         "match": {
                #             "content": query  # Assuming the field you're searching is 'content'
                #         }
                #     }
                # },
                size=250
        )
        custom_links_data = custom_links["hits"]["hits"]
        # print(response['hits'])
        json_data = []
        csv_data = []
        for data in tqdm(response['hits']['hits'], desc="Processing Elasticsearch Data"):
            print(f'data ', data['_source'])
            state_slug = f"https://houzeo.com/homes-for-sale/{data['_source']['Slug']}"
            state_data =  {}
            state_data['state_slug'] = state_slug
            
            # csv data
            csv_data.append(state_slug)

            clinks = generate_links_from_elasticsearch_data(custom_links_data,'https://houzeo.com/homes-for-sale')
            state_data['custom_links'] = clinks
            csv_data.extend(clinks)

            for city in data['_source']['City']:
                city_slug = f"{state_slug}/{city['Slug']}"
                state_data['city_slug'] = city_slug
                
                city_clinks = generate_links_from_elasticsearch_data(custom_links_data, city_slug)
                state_data['city_custom_links'] = city_clinks
                csv_data.extend(city_clinks)

            for county in data['_source']['County']:
                
                county_slug = f"{state_slug}/{county['Slug']}"
                state_data['county_slug'] = county_slug
                
                county_clinks = generate_links_from_elasticsearch_data(custom_links_data, county_slug)
                state_data['county_custom_links'] = county_clinks
                csv_data.extend(city_clinks)

            for postalCode in data['_source']['PostalCode']:
                postalCode_slug = f"{state_slug}/{postalCode['Slug']}"
                state_data['postalCode_slug'] = postalCode_slug
                
                postalCode_clinks = generate_links_from_elasticsearch_data(custom_links_data, postalCode_slug)
                state_data['postalCode_custom_links'] = postalCode_clinks
                csv_data.extend(postalCode_clinks)

            json_data.append(state_data)



        store_to_csv(csv_data)
        return csv_data
        # return response['hits']
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def generate_links_from_elasticsearch_data(custom_links, link):
    clink_data = []
    
    # Assuming custom_links contains the Elasticsearch response with hits
    for hit in custom_links:
        # Access the '_source' field in each hit
        data = hit['_source']
        if data['Active'] != 1: continue

        # Generate the full URL using the 'Url' field
        clink = f"{link}/{data['Url']}"
        print(f" URL : ", data['Url']," data ", data)
        clink_data.append(clink)
    
    return clink_data

def store_to_csv(json_data):
    csv_filename = "output.csv"

    # Write the data to a CSV file
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        
         # Write each item in csv_data as a new row with a progress bar
        for item in tqdm(csv_data, desc="Writing to CSV"):
            writer.writerow([item])  # Each item is written as a single row

    print(f"Data successfully written to {csv_filename}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
