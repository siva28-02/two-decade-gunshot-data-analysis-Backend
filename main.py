from fastapi import FastAPI
import pandas as pd
import uvicorn
from fastapi.middleware.cors import CORSMiddleware
import requests
from io import StringIO
# Initialize FastAPI app
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global variable to store the DataFrame
df = None

# Public URL of the dataset
PUBLIC_URL = "https://datasetstoragecloud.s3.us-east-1.amazonaws.com/gun_violence_usa.csv"

# Map numeric months to their names
month_map = {
    1: "January", 2: "February", 3: "March", 4: "April", 
    5: "May", 6: "June", 7: "July", 8: "August", 
    9: "September", 10: "October", 11: "November", 12: "December"
}
states_dict = {
    "Virginia": "VA",    "Texas": "TX",    "Rhode Island": "RI",    "Colorado": "CO",    "Iowa": "IA",    "Alaska": "AK",
    "Utah": "UT",    "Missouri": "MO",    "Alabama": "AL",    "New Mexico": "NM",    "Hawaii": "HI",    "Kansas": "KS",
    "Tennessee": "TN",    "Arkansas": "AR",    "Indiana": "IN",    "California": "CA",    "Oregon": "OR",    "Nevada": "NV",
    "Delaware": "DE",    "Minnesota": "MN",    "Illinois": "IL",    "New Jersey": "NJ",    "New York": "NY",    "New Hampshire": "NH",
    "Florida": "FL",    "Arizona": "AZ",    "West Virginia": "WV",    "Georgia": "GA",    "North Dakota": "ND",    "Kentucky": "KY",
    "Louisiana": "LA",    "Massachusetts": "MA",    "South Carolina": "SC",    "Michigan": "MI",    "Wyoming": "WY",    "North Carolina": "NC",
    "Maryland": "MD",    "Vermont": "VT",    "Washington": "WA",    "Wisconsin": "WI",    "Ohio": "OH",    "Maine": "ME",    "Connecticut": "CT",    "South Dakota": "SD",
    "Montana": "MT",    "Mississippi": "MS",    "Oklahoma": "OK",    "Pennsylvania": "PA",    "Nebraska": "NE",    "Idaho": "ID"
}

@app.on_event("startup")
async def load_dataset():
    global df
    try:
        # Fetch the CSV data from the public URL
        response = requests.get(PUBLIC_URL)
        response.raise_for_status()  # Raise exception for HTTP errors

        # Read the content into a Pandas DataFrame
        data = StringIO(response.text)
        df = pd.read_csv(data)
        print("Dataset loaded successfully on startup.")
        # Add a new column for month names
        df["MonthName"] = df["Month"].map(month_map)
    except Exception as e:
        print(f"Failed to load dataset: {e}")

# df = pd.read_csv("assets/gun_violence_usa.csv")

@app.get("/getstates")
def get_states():
    filtered_data = df["State"].unique()
    filtered_data.sort()
    data=[]
    for i in filtered_data:
        x={}
        x["name"]=i
        x["code"]=states_dict[i]
        data.append(x)
    return {"states":data}

@app.get("/getyears")
def get_years():
    filtered_data = df["Year"].unique()
    filtered_data.sort()
    data=[]
    for i in filtered_data:
        data.append({"year":int(i),"value":int(i)})
    return {"years":data}

@app.get("/deaths/monthly")
def get_total_deaths_monthly(year: int, state: str):
    # Filter the data based on Year and State
    filtered_data = df[(df["Year"] == year) & (df["State"] == state)]

    # Group by Month and count the total deaths
    monthly_deaths = filtered_data.groupby("MonthName").size().reset_index(name="TotalDeaths")

    # Convert to JSON format
    result = monthly_deaths.to_dict(orient="records")
    return {"monthly_deaths": result}

@app.get("/deaths/yearly")
def get_total_deaths_yearly(state:str):
    # Filter the data based on Year and State
    filtered_data = df[(df["State"] == state)]

    # Group by Year and count the total deaths
    yearly_deaths = filtered_data.groupby("Year").size().reset_index(name="TotalDeaths").sort_values(by="Year", ascending=False)
    # Convert to JSON format
    result = yearly_deaths.to_dict(orient="records")
    return {"yearly_deaths": result}

@app.get("/deaths/age-group")
def get_deaths_by_age_group(year:int, state:str):
    # Filter the data based on Year and State
    filtered_data = df[(df["Year"] == year) & (df["State"] == state)]

    # Define age ranges
    bins = [0, 18, 35, 50, 65, 80, 100]
    labels = ["0-18", "19-35", "36-50", "51-65", "66-80", "81-100"]

    # Create a new column for age groups
    df["AgeGroup"] = pd.cut(filtered_data["Victim Age"], bins=bins, labels=labels, right=False)

    # Group by Age Group and Victim Gender, then count deaths
    age_gender_deaths = df.groupby(["AgeGroup", "Victim Gender"]).size().reset_index(name="Total Deaths")

    # Pivot the table to group Male and Female deaths by Age Group
    age_gender_deaths_pivot = age_gender_deaths.pivot_table(index="AgeGroup", columns="Victim Gender", values="Total Deaths", fill_value=0)

    # Convert the result into a dictionary format for easy consumption
    result = age_gender_deaths_pivot.reset_index().to_dict(orient="records")

    # Convert the result into a more readable format
    formatted_result = []
    for entry in result:
        formatted_result.append({
            "AgeGroup": entry["AgeGroup"],
            "Male": entry.get("Male", 0),
            "Female": entry.get("Female", 0),
        })

    return {"deaths_by_age_group": formatted_result}

@app.get("/deaths/police-involved")
def get_deaths_police_involved_statewise(year:int):
     # Filter the data based on Year
    filtered_data = df[(df["Year"] == year)]

    # Filter dataset where Police Involved is "Yes"
    police_deaths = filtered_data[df["Police Involved"] == "Yes"]

    # Group by State and count the number of deaths where police were involved
    statewise_police_deaths = police_deaths.groupby("State")["State"].count().reset_index(name="TotalDeathsInvolvingPolice")

    # Convert to a dictionary for easy consumption
    result = statewise_police_deaths.to_dict(orient="records")

    return {"deaths_police_involved_statewise": result}

@app.get("/deaths/top-states")
def get_top_states_by_incident_count(limit: int = 5):
    # Group by State and count the number of incidents (rows)
    statewise_incidents = df.groupby("State").size().reset_index(name="TotalIncidents")

    # Sort by the total incident count in descending order
    sorted_states = statewise_incidents.sort_values(by="TotalIncidents", ascending=False)

    # Limit the results based on the `limit` parameter
    top_states = sorted_states.head(limit)

    # Convert to a dictionary format
    result = top_states.to_dict(orient="records")

    return {"top_states_by_incident_count": result}

@app.get("/deaths/type-of-deaths")
def get_type_of_deaths(year: int, state: str):
    # Filter the data based on Year and State
    filtered_data = df[(df["Year"] == year) & (df["State"] == state)]

    # Group by Type of Deaths and count the number of incidents
    typewise_deaths = filtered_data.groupby("TypeofDeaths").size().reset_index(name="TotalDeaths")

    # Convert to a dictionary format for easy consumption
    result = typewise_deaths.to_dict(orient="records")

    return {"type_of_deaths": result}
if __name__ == "__main__": 
    uvicorn.run(app, host="0.0.0.0", port=80000)