import boto3
from boto3.dynamodb.conditions import Key, Attr
from typing import Dict, List, Tuple, Optional
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DynamoHandler:
    def __init__(self, table_name: str = 'bsu372js8xz98x-table'):
        self.dynamodb = boto3.resource('dynamodb')
        self.table = self.dynamodb.Table(table_name)
        self.table_name = table_name
        
    async def create_file(self, file_data: Dict) -> Dict:
        """
        Create a new file record in DynamoDB
        """
        try:
            # Add timestamps
            file_data['created_at'] = datetime.now().isoformat()
            file_data['updated_at'] = file_data['created_at']
            
            # Add version for optimistic locking
            file_data['version'] = 1
            
            self.table.put_item(
                Item=file_data,
                ConditionExpression='attribute_not_exists(id)'
            )
            return file_data
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            raise ValueError(f"File with id {file_data['id']} already exists")
        except Exception as e:
            logger.error(f"Error creating file in DynamoDB: {str(e)}")
            raise

    async def get_file(self, file_id: str) -> Optional[Dict]:
        """
        Get a file record by ID
        """
        try:
            response = self.table.get_item(
                Key={'id': file_id},
                ConsistentRead=True
            )
            return response.get('Item')
        except Exception as e:
            logger.error(f"Error getting file from DynamoDB: {str(e)}")
            raise

    async def list_files(
        self,
        limit: int = 100,
        last_evaluated_key: Optional[str] = None
    ) -> Tuple[List[Dict], Optional[str]]:
        """
        List files with pagination
        """
        try:
            params = {
                'Limit': limit,
                'FilterExpression': Attr('status').eq('active')
            }
            
            if last_evaluated_key:
                params['ExclusiveStartKey'] = {'id': last_evaluated_key}
            
            response = self.table.scan(**params)
            
            return (
                response.get('Items', []),
                response.get('LastEvaluatedKey', {}).get('id')
            )
        except Exception as e:
            logger.error(f"Error listing files from DynamoDB: {str(e)}")
            raise

    async def update_file(self, file_id: str, update_data: Dict) -> Dict:
        """
        Update a file record
        """
        try:
            # Get current version
            current_file = await self.get_file(file_id)
            if not current_file:
                raise ValueError(f"File with id {file_id} not found")
            
            current_version = current_file.get('version', 0)
            
            # Prepare update expression and attribute values
            update_expr = ['set updated_at = :updated_at', 'version = :new_version']
            expr_attr_values = {
                ':updated_at': datetime.now().isoformat(),
                ':current_version': current_version,
                ':new_version': current_version + 1
            }
            
            # Add other fields to update
            for key, value in update_data.items():
                if key not in ['id', 'created_at', 'updated_at', 'version']:
                    update_expr.append(f'#{key} = :{key}')
                    expr_attr_values[f':{key}'] = value
            
            # Prepare expression attribute names
            expr_attr_names = {f'#{k}': k for k in update_data.keys()}
            
            response = self.table.update_item(
                Key={'id': file_id},
                UpdateExpression='set ' + ', '.join(update_expr),
                ExpressionAttributeValues=expr_attr_values,
                ExpressionAttributeNames=expr_attr_names,
                ConditionExpression='version = :current_version',
                ReturnValues='ALL_NEW'
            )
            
            return response['Attributes']
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            raise ValueError("Concurrent update detected. Please retry the operation.")
        except Exception as e:
            logger.error(f"Error updating file in DynamoDB: {str(e)}")
            raise

    async def delete_file(self, file_id: str) -> None:
        """
        Delete a file record
        """
        try:
            self.table.delete_item(
                Key={'id': file_id},
                ConditionExpression='attribute_exists(id)'
            )
        except self.dynamodb.meta.client.exceptions.ConditionalCheckFailedException:
            raise ValueError(f"File with id {file_id} not found")
        except Exception as e:
            logger.error(f"Error deleting file from DynamoDB: {str(e)}")
            raise

    async def batch_get_files(self, file_ids: List[str]) -> List[Dict]:
        """
        Get multiple files in a batch
        """
        try:
            # DynamoDB limits batch gets to 100 items
            file_ids = file_ids[:100]
            
            response = self.dynamodb.batch_get_item(
                RequestItems={
                    self.table_name: {
                        'Keys': [{'id': file_id} for file_id in file_ids],
                        'ConsistentRead': True
                    }
                }
            )
            
            return response['Responses'][self.table_name]
        except Exception as e:
            logger.error(f"Error batch getting files from DynamoDB: {str(e)}")
            raise

    async def batch_delete_files(self, file_ids: List[str]) -> List[str]:
        """
        Delete multiple files in a batch
        """
        try:
            # DynamoDB limits batch writes to 25 items
            file_ids = file_ids[:25]
            
            with self.table.batch_writer() as batch:
                for file_id in file_ids:
                    batch.delete_item(Key={'id': file_id})
            
            return file_ids
        except Exception as e:
            logger.error(f"Error batch deleting files from DynamoDB: {str(e)}")
            raise

    async def query_files_by_date_range(
        self,
        start_date: str,
        end_date: str,
        limit: int = 100
    ) -> List[Dict]:
        """
        Query files within a date range
        """
        try:
            response = self.table.query(
                IndexName='created_at-index',  # Requires GSI on created_at
                KeyConditionExpression=Key('created_at').between(start_date, end_date),
                Limit=limit
            )
            
            return response['Items']
        except Exception as e:
            logger.error(f"Error querying files from DynamoDB: {str(e)}")
            raise