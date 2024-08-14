import yaml
import requests
from fastapi import FastAPI, UploadFile, File, HTTPException
from pydantic import BaseModel
from requests.auth import HTTPBasicAuth
from typing import Optional
from io import BytesIO

app = FastAPI()

class AuthDetails(BaseModel):
    username: str
    password: str
    auth_string: Optional[str] = None

class Metadata(BaseModel):
    base_url: str
    auth: AuthDetails
    catalog: dict
    representation: dict
    offer: dict
    resource_catalog: dict
    representation_resource: dict
    contract: dict
    rule: dict
    rule_contract: dict
    artifact: dict
    artifact_representation: dict


@app.post("/process_metadata/")
async def process_metadata(file: UploadFile = File(...)):
    try:
        content = await file.read()
        metadata = yaml.safe_load(BytesIO(content))

        # Convert YAML to Pydantic model
        metadata_model = Metadata(**metadata)

        # Base URL and authentication setup
        base_url = metadata_model.base_url
        auth = HTTPBasicAuth(metadata_model.auth.username, metadata_model.auth.password)

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {metadata_model.auth.auth_string}'
        }

        # Define a function to send POST requests
        def send_post_request(endpoint, data):
            url = f"{base_url}{endpoint}"
            response = requests.post(url, json=data, headers=headers, auth=auth)
            response.raise_for_status()
            return response.json()

        # Step-by-step requests
        catalog_response = send_post_request('/api/catalogs', metadata_model.catalog)
        representation_response = send_post_request('/api/representations', metadata_model.representation)
        offer_response = send_post_request('/api/offers', metadata_model.offer)
        resource_catalog_response = send_post_request(f"/api/catalogs/{catalog_response['id']}/offers", metadata_model.resource_catalog)
        representation_resource_response = send_post_request(f"/api/offers/{offer_response['id']}/representations", metadata_model.representation_resource)
        contract_response = send_post_request('/api/contracts', metadata_model.contract)
        rule_response = send_post_request('/api/rules', metadata_model.rule)
        rule_contract_response = send_post_request(f"/api/contracts/{contract_response['id']}/rules", metadata_model.rule_contract)
        artifact_response = send_post_request('/api/artifacts', metadata_model.artifact)
        artifact_representation_response = send_post_request(f"/api/representations/{representation_response['id']}/artifacts", metadata_model.artifact_representation)

        return {
            "catalog_response": catalog_response,
            "representation_response": representation_response,
            "offer_response": offer_response,
            "resource_catalog_response": resource_catalog_response,
            "representation_resource_response": representation_resource_response,
            "contract_response": contract_response,
            "rule_response": rule_response,
            "rule_contract_response": rule_contract_response,
            "artifact_response": artifact_response,
            "artifact_representation_response": artifact_representation_response
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
