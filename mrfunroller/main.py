# my_mrf_parser/main.py

from fastapi import FastAPI, UploadFile, File
import uvicorn
from .in_network import parse_in_network, save_to_file, read_output_file_line_by_line
from .provider_references import parse_provider_references

app = FastAPI()

output_file_path = "output_file.json"  # Define the output path globally

@app.post("/upload/")
async def upload(file: UploadFile = File(...)):
    # Save the uploaded file
    file_location = f"/tmp/{file.filename}"
    with open(file_location, "wb") as buffer:
        buffer.write(await file.read())

    # Parse the provider references
    provider_data = parse_provider_references(file_location)
    print("Data extracted from provider references")
    print(provider_data)

    # Parse the in-network data
    parse_in_network(file_location, output_file_path)
    read_output_file_line_by_line(output_file_path)

    return {"filename": file.filename}

if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)
