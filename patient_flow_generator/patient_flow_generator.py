import time
import json
import random
import uuid
from datetime import datetime, timedelta
from azure.eventhub import EventHubProducerClient, EventData

# ---------------- CONFIGURATION  ---------------- #
CONNECTION_STRING = "Endpoint=sb://hospital-flow-ns-austin.servicebus.windows.net/;SharedAccessKeyName=producer_policy;sdfsfSharedAccessKey=B7mX2XS0RPGF8+HKT5bN+ouHS+a3GC1Sh+AEhI/ZxiY=;EntityPath=patient-admission-stream"  # Primary Key starting with Endpoint=sb://...
EVENT_HUB_NAME = "patient-admission-stream"             
# ---------------------------------------------------------------------- #

# Hospital Data
DEPARTMENTS = ["Emergency", "ICU", "Cardiology", "Neurology", "Pediatrics", "Oncology", "Orthopedics"]
GENDERS = ["Male", "Female", "Other"]
HOSPITAL_IDS = list(range(1, 8)) # 7 Hospitals in the network

def generate_patient_event():
    """Generates a single patient admission/discharge record."""
    
    # Simulate time: Mostly current, but sometimes slight delays (past)
    admission_time = datetime.utcnow() - timedelta(hours=random.randint(0, 50))
    discharge_time = admission_time + timedelta(hours=random.randint(4, 72))

    record = {
        "patient_id": str(uuid.uuid4()),
        "gender": random.choice(GENDERS),
        "age": random.randint(1, 90),  # Valid age range
        "department": random.choice(DEPARTMENTS),
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(100, 999),
        "hospital_id": random.choice(HOSPITAL_IDS)
    }
    return record

def ingest_dirty_data():
    """Generates 'Bad Data' intentionally (5% chance) to test the cleaning pipeline."""
    record = generate_patient_event()
    
    # Error Type 1: Invalid Age (> 100)
    record['age'] = random.randint(110, 150)
    
    # Error Type 2: Future Admission Date (Impossible)
    record['admission_time'] = (datetime.utcnow() + timedelta(days=5)).isoformat()
    
    return record

def main():
    print("Initialize Event Hub Producer...")
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING, 
        eventhub_name=EVENT_HUB_NAME
    )

    print("Sending events to Azure Event Hub... Press Ctrl+C to stop.")
    
    with producer:
        while True:
            # 5% chance to send dirty data, 95% chance for clean data
            if random.random() < 0.05:
                event_data = ingest_dirty_data()
                print(f"[DIRTY DATA SENT] {event_data}")
            else:
                event_data = generate_patient_event()
                print(f"[Sent]: {event_data['patient_id']} - {event_data['department']}")

            # Create a batch and add the event
            event_batch = producer.create_batch()
            event_batch.add(EventData(json.dumps(event_data)))
            
            # Send the batch
            producer.send_batch(event_batch)
            
            # Wait 1-3 seconds before sending the next record.seems natural
            time.sleep(random.randint(1, 3))

if __name__ == "__main__":
    main()