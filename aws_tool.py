import json
import boto3
import base64
from typing import Any, Dict, List, Optional
from botocore.exceptions import ClientError, BotoCoreError
from mcp.server.fastmcp import FastMCP
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Initialize FastMCP server
mcp = FastMCP("aws_tools")

# Thread pool for running boto3 operations (which are synchronous) in async context
executor = ThreadPoolExecutor(max_workers=5)

# AWS clients - will be initialized based on credentials
lambda_client = None
s3_client = None
s3_resource = None

# Get AWS credentials from environment variables (secure way)
aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
region_name = os.getenv("AWS_DEFAULT_REGION", "ap-south-1")

def initialize_aws_clients(aws_access_key_id: str = None, aws_secret_access_key: str = None, 
                          region_name: str = None):
    """Initialize AWS clients with credentials"""
    global lambda_client, s3_client, s3_resource
    
    try:
        # Use provided credentials or get from environment variables
        access_key = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY")
        region = region_name or os.getenv("AWS_DEFAULT_REGION", "ap-south-1")
        
        session_kwargs = {'region_name': region}
        if access_key and secret_key:
            session_kwargs.update({
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key
            })
        
        session = boto3.Session(**session_kwargs)
        
        lambda_client = session.client('lambda')
        s3_client = session.client('s3')
        s3_resource = session.resource('s3')
        
        print(f"✅ AWS clients initialized successfully for region: {region}")
        return True
    except Exception as e:
        print(f"❌ Failed to initialize AWS clients: {e}")
        return False

def run_sync_in_async(func, *args, **kwargs):
    """Run synchronous boto3 functions in async context"""
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(executor, func, *args, **kwargs)

# Initialize clients on startup (using environment variables)
if aws_access_key_id and aws_secret_access_key:
    initialize_aws_clients(aws_access_key_id, aws_secret_access_key, region_name)
    print(f"🔐 AWS credentials loaded from environment variables")
else:
    print("⚠️  No AWS credentials found in environment variables.")
    print("   Please set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and AWS_DEFAULT_REGION")
    print("   or use the configure_aws_credentials tool after starting the server.")

@mcp.tool()
async def configure_aws_credentials(aws_access_key_id: str, aws_secret_access_key: str, 
                                  region_name: str = 'us-east-1') -> str:
    """Configure AWS credentials for the MCP server
    
    Args:
        aws_access_key_id: Your AWS Access Key ID
        aws_secret_access_key: Your AWS Secret Access Key
        region_name: AWS region (default: us-east-1)
    """
    try:
        success = initialize_aws_clients(aws_access_key_id, aws_secret_access_key, region_name)
        if success:
            return json.dumps({
                "success": True,
                "message": "AWS credentials configured successfully",
                "region": region_name
            })
        else:
            return json.dumps({
                "success": False,
                "error": "Failed to initialize AWS clients"
            })
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to configure credentials: {str(e)}"
        })

# ============= LAMBDA FUNCTIONS =============

@mcp.tool()
async def list_lambda_functions(max_items: int = 50) -> str:
    """List all Lambda functions in the account
    
    Args:
        max_items: Maximum number of functions to return (default: 50)
    """
    if not lambda_client:
        return json.dumps({"error": "Lambda client not initialized. Configure AWS credentials first."})
    
    try:
        def _list_functions():
            paginator = lambda_client.get_paginator('list_functions')
            functions = []
            
            for page in paginator.paginate(PaginationConfig={'MaxItems': max_items}):
                for func in page['Functions']:
                    functions.append({
                        'name': func['FunctionName'],
                        'runtime': func['Runtime'],
                        'handler': func['Handler'],
                        'description': func.get('Description', ''),
                        'last_modified': func['LastModified'],
                        'memory_size': func['MemorySize'],
                        'timeout': func['Timeout'],
                        'arn': func['FunctionArn']
                    })
            
            return functions
        
        functions = await run_sync_in_async(_list_functions)
        
        return json.dumps({
            "success": True,
            "functions": functions,
            "count": len(functions)
        }, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to list Lambda functions: {str(e)}"
        })

@mcp.tool()
async def invoke_lambda_function(function_name: str, payload: str = None, 
                                invocation_type: str = 'RequestResponse') -> str:
    """Invoke a Lambda function
    
    Args:
        function_name: Name or ARN of the Lambda function
        payload: JSON payload to send to the function (optional)
        invocation_type: RequestResponse (synchronous) or Event (asynchronous)
    """
    if not lambda_client:
        return json.dumps({"error": "Lambda client not initialized. Configure AWS credentials first."})
    
    try:
        def _invoke_function():
            invoke_args = {
                'FunctionName': function_name,
                'InvocationType': invocation_type
            }
            
            if payload:
                # Validate JSON payload
                try:
                    json.loads(payload)
                    invoke_args['Payload'] = payload
                except json.JSONDecodeError:
                    raise ValueError("Invalid JSON payload")
            
            response = lambda_client.invoke(**invoke_args)
            
            result = {
                'status_code': response['StatusCode'],
                'execution_result': response.get('ExecutedVersion', 'Unknown')
            }
            
            if 'Payload' in response:
                payload_content = response['Payload'].read().decode('utf-8')
                try:
                    result['response_payload'] = json.loads(payload_content)
                except json.JSONDecodeError:
                    result['response_payload'] = payload_content
            
            if 'LogResult' in response:
                result['logs'] = base64.b64decode(response['LogResult']).decode('utf-8')
            
            if 'FunctionError' in response:
                result['function_error'] = response['FunctionError']
            
            return result
        
        result = await run_sync_in_async(_invoke_function)
        
        return json.dumps({
            "success": True,
            "function_name": function_name,
            "invocation_type": invocation_type,
            "result": result
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to invoke Lambda function '{function_name}': {str(e)}"
        })

@mcp.tool()
async def get_lambda_function_info(function_name: str) -> str:
    """Get detailed information about a Lambda function
    
    Args:
        function_name: Name or ARN of the Lambda function
    """
    if not lambda_client:
        return json.dumps({"error": "Lambda client not initialized. Configure AWS credentials first."})
    
    try:
        def _get_function():
            response = lambda_client.get_function(FunctionName=function_name)
            
            config = response['Configuration']
            code = response.get('Code', {})
            
            function_info = {
                'name': config['FunctionName'],
                'arn': config['FunctionArn'],
                'runtime': config['Runtime'],
                'handler': config['Handler'],
                'description': config.get('Description', ''),
                'memory_size': config['MemorySize'],
                'timeout': config['Timeout'],
                'last_modified': config['LastModified'],
                'version': config['Version'],
                'state': config['State'],
                'code_size': config['CodeSize'],
                'environment_variables': config.get('Environment', {}).get('Variables', {}),
                'vpc_config': config.get('VpcConfig', {}),
                'role': config['Role'],
                'repository_type': code.get('RepositoryType'),
                'location': code.get('Location')
            }
            
            return function_info
        
        function_info = await run_sync_in_async(_get_function)
        
        return json.dumps({
            "success": True,
            "function_info": function_info
        }, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to get Lambda function info for '{function_name}': {str(e)}"
        })

# ============= S3 FUNCTIONS =============

@mcp.tool()
async def list_s3_buckets() -> str:
    """List all S3 buckets in the account"""
    if not s3_client:
        return json.dumps({"error": "S3 client not initialized. Configure AWS credentials first."})
    
    try:
        def _list_buckets():
            response = s3_client.list_buckets()
            buckets = []
            
            for bucket in response['Buckets']:
                buckets.append({
                    'name': bucket['Name'],
                    'creation_date': bucket['CreationDate']
                })
            
            return buckets
        
        buckets = await run_sync_in_async(_list_buckets)
        
        return json.dumps({
            "success": True,
            "buckets": buckets,
            "count": len(buckets)
        }, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to list S3 buckets: {str(e)}"
        })

@mcp.tool()
async def list_s3_objects(bucket_name: str, prefix: str = "", max_keys: int = 100) -> str:
    """List objects in an S3 bucket
    
    Args:
        bucket_name: Name of the S3 bucket
        prefix: Prefix to filter objects (optional)
        max_keys: Maximum number of objects to return (default: 100)
    """
    if not s3_client:
        return json.dumps({"error": "S3 client not initialized. Configure AWS credentials first."})
    
    try:
        def _list_objects():
            list_args = {
                'Bucket': bucket_name,
                'MaxKeys': min(max_keys, 1000)  # AWS limit
            }
            
            if prefix:
                list_args['Prefix'] = prefix
            
            response = s3_client.list_objects_v2(**list_args)
            
            objects = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    objects.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'etag': obj['ETag'].strip('"'),
                        'storage_class': obj.get('StorageClass', 'STANDARD')
                    })
            
            return {
                'objects': objects,
                'is_truncated': response.get('IsTruncated', False),
                'key_count': response.get('KeyCount', 0)
            }
        
        result = await run_sync_in_async(_list_objects)
        
        return json.dumps({
            "success": True,
            "bucket_name": bucket_name,
            "prefix": prefix,
            **result
        }, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to list objects in bucket '{bucket_name}': {str(e)}"
        })

@mcp.tool()
async def get_s3_object(bucket_name: str, object_key: str, encoding: str = 'utf-8') -> str:
    """Get content of an S3 object
    
    Args:
        bucket_name: Name of the S3 bucket
        object_key: Key (path) of the object
        encoding: Text encoding for the content (default: utf-8)
    """
    if not s3_client:
        return json.dumps({"error": "S3 client not initialized. Configure AWS credentials first."})
    
    try:
        def _get_object():
            response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
            
            # Get object metadata
            metadata = {
                'content_type': response.get('ContentType', 'unknown'),
                'content_length': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag', '').strip('"'),
                'metadata': response.get('Metadata', {})
            }
            
            # Read content
            content = response['Body'].read()
            
            # Try to decode as text if possible
            try:
                if encoding:
                    decoded_content = content.decode(encoding)
                    is_binary = False
                else:
                    decoded_content = None
                    is_binary = True
            except UnicodeDecodeError:
                decoded_content = None
                is_binary = True
            
            return {
                'metadata': metadata,
                'content': decoded_content,
                'content_base64': base64.b64encode(content).decode('utf-8') if is_binary else None,
                'is_binary': is_binary,
                'size_bytes': len(content)
            }
        
        result = await run_sync_in_async(_get_object)
        
        return json.dumps({
            "success": True,
            "bucket_name": bucket_name,
            "object_key": object_key,
            **result
        }, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to get object '{object_key}' from bucket '{bucket_name}': {str(e)}"
        })

@mcp.tool()
async def upload_s3_object(bucket_name: str, object_key: str, content: str, 
                          content_type: str = 'text/plain', is_base64: bool = False) -> str:
    """Upload content to an S3 object
    
    Args:
        bucket_name: Name of the S3 bucket
        object_key: Key (path) for the object
        content: Content to upload (text or base64 encoded binary)
        content_type: MIME type of the content (default: text/plain)
        is_base64: Whether the content is base64 encoded binary data
    """
    if not s3_client:
        return json.dumps({"error": "S3 client not initialized. Configure AWS credentials first."})
    
    try:
        def _upload_object():
            # Prepare content
            if is_base64:
                body = base64.b64decode(content)
            else:
                body = content.encode('utf-8')
            
            response = s3_client.put_object(
                Bucket=bucket_name,
                Key=object_key,
                Body=body,
                ContentType=content_type
            )
            
            return {
                'etag': response.get('ETag', '').strip('"'),
                'version_id': response.get('VersionId'),
                'size_bytes': len(body)
            }
        
        result = await run_sync_in_async(_upload_object)
        
        return json.dumps({
            "success": True,
            "bucket_name": bucket_name,
            "object_key": object_key,
            "content_type": content_type,
            **result
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to upload object '{object_key}' to bucket '{bucket_name}': {str(e)}"
        })

@mcp.tool()
async def delete_s3_object(bucket_name: str, object_key: str) -> str:
    """Delete an S3 object
    
    Args:
        bucket_name: Name of the S3 bucket
        object_key: Key (path) of the object to delete
    """
    if not s3_client:
        return json.dumps({"error": "S3 client not initialized. Configure AWS credentials first."})
    
    try:
        def _delete_object():
            response = s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            return {
                'delete_marker': response.get('DeleteMarker', False),
                'version_id': response.get('VersionId')
            }
        
        result = await run_sync_in_async(_delete_object)
        
        return json.dumps({
            "success": True,
            "bucket_name": bucket_name,
            "object_key": object_key,
            "message": f"Object '{object_key}' deleted successfully",
            **result
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to delete object '{object_key}' from bucket '{bucket_name}': {str(e)}"
        })

@mcp.tool()
async def get_s3_object_metadata(bucket_name: str, object_key: str) -> str:
    """Get metadata of an S3 object without downloading the content
    
    Args:
        bucket_name: Name of the S3 bucket
        object_key: Key (path) of the object
    """
    if not s3_client:
        return json.dumps({"error": "S3 client not initialized. Configure AWS credentials first."})
    
    try:
        def _head_object():
            response = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            
            return {
                'content_type': response.get('ContentType', 'unknown'),
                'content_length': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified'),
                'etag': response.get('ETag', '').strip('"'),
                'version_id': response.get('VersionId'),
                'storage_class': response.get('StorageClass', 'STANDARD'),
                'metadata': response.get('Metadata', {}),
                'cache_control': response.get('CacheControl'),
                'content_encoding': response.get('ContentEncoding'),
                'expires': response.get('Expires')
            }
        
        result = await run_sync_in_async(_head_object)
        
        return json.dumps({
            "success": True,
            "bucket_name": bucket_name,
            "object_key": object_key,
            "metadata": result
        }, indent=2, default=str)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to get metadata for object '{object_key}' in bucket '{bucket_name}': {str(e)}"
        })

@mcp.tool()
async def generate_presigned_url(bucket_name: str, object_key: str, expiration: int = 3600, 
                               http_method: str = 'GET') -> str:
    """Generate a presigned URL for an S3 object
    
    Args:
        bucket_name: Name of the S3 bucket
        object_key: Key (path) of the object
        expiration: URL expiration time in seconds (default: 3600 = 1 hour)
        http_method: HTTP method (GET, PUT, DELETE, etc.)
    """
    if not s3_client:
        return json.dumps({"error": "S3 client not initialized. Configure AWS credentials first."})
    
    try:
        def _generate_url():
            url = s3_client.generate_presigned_url(
                ClientMethod='get_object' if http_method.upper() == 'GET' else 'put_object',
                Params={'Bucket': bucket_name, 'Key': object_key},
                ExpiresIn=expiration
            )
            return url
        
        presigned_url = await run_sync_in_async(_generate_url)
        
        return json.dumps({
            "success": True,
            "bucket_name": bucket_name,
            "object_key": object_key,
            "presigned_url": presigned_url,
            "http_method": http_method,
            "expires_in_seconds": expiration,
            "expires_at": (datetime.now().timestamp() + expiration)
        }, indent=2)
        
    except Exception as e:
        return json.dumps({
            "success": False,
            "error": f"Failed to generate presigned URL for '{object_key}' in bucket '{bucket_name}': {str(e)}"
        })

if __name__ == "__main__":
    print("🚀 Starting AWS MCP server...")
    print("📋 Available tools:")
    print("   - Lambda: list_lambda_functions, invoke_lambda_function, get_lambda_function_info")
    print("   - S3: list_s3_buckets, list_s3_objects, get_s3_object, upload_s3_object, delete_s3_object")
    print("   - Utilities: configure_aws_credentials, get_s3_object_metadata, generate_presigned_url")
    print("\n🔧 AWS Configuration:")
    if lambda_client and s3_client:
        print("   ✅ AWS clients initialized and ready!")
    else:
        print("   ⚠️  AWS clients not initialized. Use configure_aws_credentials tool.")
    print("\n" + "="*60)
    mcp.run(transport='stdio')
