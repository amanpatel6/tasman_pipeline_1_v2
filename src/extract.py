import requests
import json 
import pathlib

def extract():
    url = "https://api.openbrewerydb.org/v1/breweries"

    response = requests.get(url)
    # print(response.json())
    # print(response.status_code)

    assert response.status_code == 200, f"Expected 200, receieved {response.status_code}"
    assert len(response.json()) > 0, "No API Response"

    all_breweries = []
    page = 1

    while True:
        response = requests.get(url, params={"page" : page, "per_page" : 200}) # calls API and says what page to start with and how many per page
        data = response.json() # converts response to json and saves to variable called data

        if len(data) == 0:
            break
        else:
            all_breweries += data # adds data to list
            page += 1 # when the loops starts again it will then start from the next page

    print(len(all_breweries))
    print(all_breweries[0])

    output_path = pathlib.Path("bronze/raw_breweries.json") # defining the output path for the json
    output_path.parent.mkdir(parents=True, exist_ok=True) # creating the bronze and data folders

    with open(output_path, "w") as f:
        json.dump(all_breweries, f) # writes out json to output path

if __name__ == "__main__": extract() # allows you to call the function and run it when you type in python src/extract.py in terminal. without this it wont call the function and give you an output
