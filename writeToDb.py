# Gender
gender_data = {
    "total_male": 0,
    "total_female": 0,
}

# Age group
age_group_data = {
    "<20": 0,
    "20s": 0,
    "30s": 0,
    "40s": 0,
    "50s": 0,
    "60s": 0,
    "70s": 0,
    "80s": 0,
    "90s": 0,
    "Unknown": 0,
}

# Acquisition type
acquisition_type_data = {
    "community_spread": 0,
    "travel_related": 0,
    "neither": 0,
    "unknown": 0,
}

# Outcome_data
outcome_type_data = {
    "active_cases": 0,
    "recovered_cases": 0,
    "fatal_cases": 0,
}

# Reporting city data
reporting_city_data = {}

# Write data to firestore db


def writeCovidDataToDb(limit, write_offset, total_results, records, db):

    for record in records[limit:limit+write_offset]:
        id = record["ROW_ID"]
        episode_date = record["ACCURATE_EPISODE_DATE"]
        age_group = record["Age_Group"]
        gender = record["CLIENT_GENDER"]
        acquisition_info = record["CASE_ACQUISITIONINFO"]
        case_outcome = record["OUTCOME1"]
        reporting_phu_address = record["Reporting_PHU_Address"]
        reporting_phu_city = record["Reporting_PHU_City"]
        reporting_phu_latitude = record["Reporting_PHU_Latitude"]
        reporting_phu_longitude = record["Reporting_PHU_Longitude"]
        reporting_phu_postal_code = record["Reporting_PHU_Postal_Code"]

        # Create emergent data here
        # postGenderData(db, gender)
        # postAgeGroupData(db, age_group)
        # postacquisitionData(db, acquisition_info)
        # postOutcomeData(db, case_outcome)
        postReportingCityData(db, reporting_phu_city)

        # Post emergent data to db

        # Finally post raw data to db
        # postRawDataToDb(db, limit, write_offset, total_results)

    print(reporting_city_data)


def postRawDataToDb(db, limit, write_offset, total_results):
    data_to_write = []
    data_to_write.append({
        "id": id,
        "episode_date": episode_date,
        "age_group": age_group,
        "gender": gender,
        "case_outcome": case_outcome,
        "reporting_phu_address": reporting_phu_address,
        "reporting_phu_city": reporting_phu_city,
        "reporting_phu_latitude": reporting_phu_latitude,
        "reporting_phu_longitude": reporting_phu_longitude,
        "reporting_phu_postal_code": reporting_phu_postal_code,
    })

    # Write to cloud firestore
    LOWER_LIMIT = limit
    UPPER_LIMIT = min(limit + write_offset, total_results)

    generic_data_ref = db.collection(
        u'ontarioData').document(f'range-{LOWER_LIMIT}-{UPPER_LIMIT}')

    # Post to db
    # generic_data_ref.set({
    #     "data": data_to_write
    # })


def postGenderData(db, gender):
    if(gender == "MALE"):
        gender_data["total_male"] += 1
    elif(gender == "FEMALE"):
        gender_data["total_female"] += 1


def postAgeGroupData(db, age_group):
    if(age_group == "<20"):
        age_group_data["<20"] += 1
    elif(age_group == "20s"):
        age_group_data["20s"] += 1
    elif(age_group == "30s"):
        age_group_data["30s"] += 1
    elif(age_group == "40s"):
        age_group_data["40s"] += 1
    elif(age_group == "50s"):
        age_group_data["50s"] += 1
    elif(age_group == "60s"):
        age_group_data["60s"] += 1
    elif(age_group == "70s"):
        age_group_data["70s"] += 1
    elif(age_group == "80s"):
        age_group_data["80s"] += 1
    elif(age_group == "90s"):
        age_group_data["90s"] += 1
    elif(age_group == "Unknown"):
        age_group_data["Unknown"] += 1


def postacquisitionData(db, acquisition_info):
    if(acquisition_info == "Contact of a confirmed case"):
        acquisition_type_data["community_spread"] += 1
    elif(acquisition_info == "Travel-Related"):
        acquisition_type_data["travel_related"] += 1
    elif(acquisition_info == "Information pending"):
        acquisition_type_data["unknown"] += 1
    elif(acquisition_info == "Neither"):
        acquisition_type_data["neither"] += 1


def postOutcomeData(db, case_outcome):
    if(case_outcome == "Not Resolved"):
        outcome_type_data["active_cases"] += 1
    elif(case_outcome == "Resolved"):
        outcome_type_data["recovered_cases"] += 1
    elif(case_outcome == "Fatal"):
        outcome_type_data["fatal_cases"] += 1


def postReportingCityData(db, reporting_phu_city):
    if(reporting_phu_city in reporting_city_data):
        reporting_city_data[reporting_phu_city] += 1
    else:
        reporting_city_data[reporting_phu_city] = 1
