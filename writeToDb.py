# Write data to firestore db
def writeCovidDataToDb(limit, write_offset, total_results, records):
    data_to_write = []

    for record in records[limit:limit+write_offset]:
        id = record["ROW_ID"]
        episode_date = record["ACCURATE_EPISODE_DATE"]
        age_group = record["Age_Group"]
        gender = record["CLIENT_GENDER"]
        case_outcome = record["OUTCOME1"]
        reporting_phu_address = record["Reporting_PHU_Address"]
        reporting_phu_city = record["Reporting_PHU_City"]
        reporting_phu_latitude = record["Reporting_PHU_Latitude"]
        reporting_phu_longitude = record["Reporting_PHU_Longitude"]
        reporting_phu_postal_code = record["Reporting_PHU_Postal_Code"]

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

    generic_data_ref.set({
        "data": data_to_write
    })


def helloWorld():
    print("Yes I have been called")
