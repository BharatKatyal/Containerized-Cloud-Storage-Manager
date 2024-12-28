from fastapi import FastAPI, File, UploadFile, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from typing import Optional, Dict, List
import boto3
import uuid
import os
from datetime import datetime
from utils.dynamo_handler import DynamoHandler
from botocore.exceptions import ClientError
import logging
import io

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="File Management API")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize AWS clients
s3 = boto3.client('s3')
BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'your-bucket-name')

# Initialize DynamoDB handler
dynamo_handler = DynamoHandler()

@app.middleware("http")
async def error_handling_middleware(request: Request, call_next):
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(e)}
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)) -> Dict:
    """
    Upload a file to S3 and store metadata in DynamoDB
    """
    try:
        file_id = str(uuid.uuid4())
        content_type = file.content_type or 'application/octet-stream'
        
        # Upload to S3
        s3.upload_fileobj(
            file.file,
            BUCKET_NAME,
            file_id,
            ExtraArgs={
                'ContentType': content_type,
                'Metadata': {
                    'original_filename': file.filename
                }
            }
        )
        
        # Get file size from S3
        response = s3.head_object(Bucket=BUCKET_NAME, Key=file_id)
        file_size = response['ContentLength']
        
        # Prepare file metadata
        file_metadata = {
            'id': file_id,
            'name': file.filename,
            'size': file_size,
            'content_type': content_type,
            'last_modified': datetime.now().isoformat(),
            's3_key': file_id,
            'status': 'active'
        }
        
        # Store in DynamoDB
        await dynamo_handler.create_file(file_metadata)
        
        return {
            "message": "File uploaded successfully",
            "file_id": file_id,
            "metadata": file_metadata
        }
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files")
async def list_files(
    limit: int = 100,
    last_evaluated_key: Optional[str] = None
) -> Dict:
    """
    List all files with pagination
    """
    try:
        files, last_key = await dynamo_handler.list_files(limit, last_evaluated_key)
        return {
            "files": files,
            "last_evaluated_key": last_key,
            "count": len(files)
        }
    except Exception as e:
        logger.error(f"List error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/files/{file_id}")
async def get_file(file_id: str) -> Dict:
    """
    Get file metadata and generate download URL
    """
    try:
        # Get metadata from DynamoDB
        file_metadata = await dynamo_handler.get_file(file_id)
        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Generate presigned URL for download
        url = s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': BUCKET_NAME, 'Key': file_id},
            ExpiresIn=3600  # URL expires in 1 hour
        )
        
        return {
            "metadata": file_metadata,
            "download_url": url
        }
    except Exception as e:
        logger.error(f"Get file error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/files/{file_id}")
async def delete_file(file_id: str) -> Dict:
    """
    Delete file from S3 and DynamoDB
    """
    try:
        # Check if file exists
        file_metadata = await dynamo_handler.get_file(file_id)
        if not file_metadata:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Delete from S3
        s3.delete_object(Bucket=BUCKET_NAME, Key=file_id)
        
        # Delete from DynamoDB
        await dynamo_handler.delete_file(file_id)
        
        return {"message": "File deleted successfully", "file_id": file_id}
    except Exception as e:
        logger.error(f"Delete error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.put("/files/{file_id}")
async def update_file(file_id: str, file_data: Dict) -> Dict:
    """
    Update file metadata
    """
    try:
        # Check if file exists
        existing_file = await dynamo_handler.get_file(file_id)
        if not existing_file:
            raise HTTPException(status_code=404, detail="File not found")
        
        # Update allowed fields
        allowed_updates = ['name', 'description', 'tags']
        update_data = {
            k: v for k, v in file_data.items() 
            if k in allowed_updates
        }
        
        update_data['last_modified'] = datetime.now().isoformat()
        
        # Update in DynamoDB
        updated_file = await dynamo_handler.update_file(file_id, update_data)
        
        return {
            "message": "File updated successfully",
            "file": updated_file
        }
    except Exception as e:
        logger.error(f"Update error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/files/batch")
async def batch_operation(operations: List[Dict]) -> Dict:
    """
    Perform batch operations on files
    """
    try:
        results = []
        for operation in operations:
            op_type = operation.get('type')
            file_id = operation.get('file_id')
            
            if op_type == 'delete':
                await delete_file(file_id)
                results.append({"file_id": file_id, "operation": "delete", "status": "success"})
            else:
                results.append({"file_id": file_id, "operation": op_type, "status": "unsupported"})
        
        return {"results": results}
    except Exception as e:
        logger.error(f"Batch operation error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5001)