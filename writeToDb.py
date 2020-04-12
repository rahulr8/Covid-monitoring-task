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
        createGenderData(db, gender)
        createAgeGroupData(db, age_group)
        createacquisitionData(db, acquisition_info)
        createOutcomeData(db, case_outcome)
        createReportingCityData(db, reporting_phu_city)

    # Post extracted data to db
    postExtractedData(
        db,
        gender_data,
        age_group_data,
        acquisition_type_data,
        outcome_type_data,
        reporting_city_data,
    )

    # Uncomment if you want to post raw data to db
    # data_to_write = []
    # data_to_write.append({
    #     "id": id,
    #     "episode_date": episode_date,
    #     "age_group": age_group,
    #     "gender": gender,
    #     "case_outcome": case_outcome,
    #     "reporting_phu_address": reporting_phu_address,
    #     "reporting_phu_city": reporting_phu_city,
    #     "reporting_phu_latitude": reporting_phu_latitude,
    #     "reporting_phu_longitude": reporting_phu_longitude,
    #     "reporting_phu_postal_code": reporting_phu_postal_code,
    # })

    # Write to cloud firestore
    # LOWER_LIMIT = limit
    # UPPER_LIMIT = min(limit + write_offset, total_results)

    # generic_data_ref = db.collection(
    #     u'ontarioData').document(f'range-{LOWER_LIMIT}-{UPPER_LIMIT}')

    # Post to db
    # generic_data_ref.set({
    #     "data": data_to_write
    # })


def createGenderData(db, gender):
    if(gender == "MALE"):
        gender_data["total_male"] += 1
    elif(gender == "FEMALE"):
        gender_data["total_female"] += 1

    return gender_data


def createAgeGroupData(db, age_group):
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

    return age_group_data


def createacquisitionData(db, acquisition_info):
    if(acquisition_info == "Contact of a confirmed case"):
        acquisition_type_data["community_spread"] += 1
    elif(acquisition_info == "Travel-Related"):
        acquisition_type_data["travel_related"] += 1
    elif(acquisition_info == "Information pending"):
        acquisition_type_data["unknown"] += 1
    elif(acquisition_info == "Neither"):
        acquisition_type_data["neither"] += 1

    return acquisition_type_data


def createOutcomeData(db, case_outcome):
    if(case_outcome == "Not Resolved"):
        outcome_type_data["active_cases"] += 1
    elif(case_outcome == "Resolved"):
        outcome_type_data["recovered_cases"] += 1
    elif(case_outcome == "Fatal"):
        outcome_type_data["fatal_cases"] += 1

    return outcome_type_data


def createReportingCityData(db, reporting_phu_city):
    if(reporting_phu_city in reporting_city_data):
        reporting_city_data[reporting_phu_city] += 1
    else:
        reporting_city_data[reporting_phu_city] = 1

    return reporting_city_data


def postExtractedData(db, data_for_gender, data_for_age, data_for_acquisition, data_for_outcome, data_for_city):
    batch = db.batch()
    data_for_gender_ref = db.collection(
        u'ontarioData').document(u'data_for_gender')
    # data_for_gender_ref.set(gender_data)
    batch.set(data_for_gender_ref, gender_data)

    data_for_age_ref = db.collection(
        u'ontarioData').document(u'data_for_age_group')
    # data_for_age_ref.set(age_group_data)
    batch.set(data_for_age_ref, age_group_data)

    data_for_acquisition_type_ref = db.collection(
        u'ontarioData').document(u'data_for_acquisition_type')
    # data_for_acquisition_type_ref.set(acquisition_type_data)
    batch.set(data_for_acquisition_type_ref, acquisition_type_data)

    data_for_outcome_type_ref = db.collection(
        u'ontarioData').document(u'data_for_outcome_type')
    # data_for_outcome_type_ref.set(outcome_type_data)
    batch.set(data_for_outcome_type_ref, outcome_type_data)

    data_for_reporting_city_ref = db.collection(
        u'ontarioData').document(u'data_for_reporting_city')
    # data_for_reporting_city_ref.set(reporting_city_data)
    batch.set(data_for_reporting_city_ref, reporting_city_data)

    batch.commit()
