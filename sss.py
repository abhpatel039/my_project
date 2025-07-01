import csv
from io import StringIO
from fastapi import FastAPI, UploadFile, File, HTTPException
import psycopg2

app = FastAPI()

# --- PostgreSQL Connection ---
def create_connection():
    return psycopg2.connect(
        host='localhost',
        database='postgres',
        user='postgres',
        password='abhi'
    )

# --- Circuit ID Logic ---
def get_circuit_id(nss_id, service_type):
    suffix_map = {
        "2G_ABIS": "_2GSX",
        "2G_OAM": "_2GMX",
        "4G_OAM": "_4GMX",
        "4G_S1_C": "_4GCX",
        "4G_S1-U": "_4GUX",
        "5G_OAM": "_5GMX",
        "5G_S1_C": "_5GCX",
        "5G_S1_U": "_5GUX"
    }
    suffix = suffix_map.get(service_type)
    if suffix and len(nss_id) >= 2:
        return f"{nss_id[2:]}{suffix}"
    return None

# --- Service Type Extractor ---
def get_service_type_c(row):
    return row[11] + "_" + row[12]

# --- Optional Validation (can expand) ---
def check_validity(circuit_id, circle_name, vlan, service_type_c, optics_router_hostname, router_ip, site_status, nss_id):
    # Add validation logic if needed
    if not circuit_id or not circle_name or not vlan or not service_type_c or not optics_router_hostname or not router_ip or not site_status or not nss_id:
        return False
            
    return True

# --- CSV Processing ---
def process_csv(file_content):
    reader = csv.reader(StringIO(file_content.decode('utf-8')))
    headers = next(reader)  # Skip headers
    data = []

    for row in reader:
        nss_id = row[21]
        service_type_c = get_service_type_c(row)
        circuit_id = get_circuit_id(nss_id, service_type_c)

        cleaned = {
            "circle_name": row[1],
            "circuit_id": circuit_id,
            "nss_id": nss_id,
            "vlan": row[15],
            "service_type_c": service_type_c,
            "optics_router_hostname": row[23],
            "router_ip": row[25],
            "site_status": "live"
        }

        if check_validity(**cleaned):
            data.append(cleaned)

    return data

# --- FastAPI Route ---
@app.post("/upload_csv/")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Please upload a .csv file")

    contents = await file.read()
    cleaned_data = process_csv(contents)
    print(cleaned_data)
    # conn = create_connection()
    # inserted_rows = 0

    # try:
    #     with conn.cursor() as cur:
    #         for row in cleaned_data:
    #             cur.execute("""
    #                 INSERT INTO uim_assurance_data
    #                 (circle_name, circuit_id, nss_id, vlan, service_type_c, optics_router_hostname, router_ip, site_status)
    #                 VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #             """, (
    #                 row['circle_name'], row['circuit_id'], row['nss_id'], row['vlan'],
    #                 row['service_type_c'], row['optics_router_hostname'], row['router_ip'], row['site_status']
    #             ))
    #             inserted_rows += 1
    #     conn.commit()
    # except Exception as e:
    #     conn.rollback()
    #     raise HTTPException(status_code=500, detail=f"Database Error: {str(e)}")
    # finally:
    #     conn.close()

    # return {"status": "success", "inserted_rows": inserted_rows}
    return {"status": "success", "inserted_rows": len(cleaned_data)}