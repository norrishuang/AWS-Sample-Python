#!/usr/bin/env python3
import sys
import json

def main():
    # Simple MCP server implementation
    while True:
        try:
            line = sys.stdin.readline()
            if not line:
                break
                
            request = json.loads(line)
            
            if request.get("method") == "list_tools":
                response = {
                    "id": request.get("id"),
                    "result": {
                        "tools": [
                            {
                                "name": "search_documentation",
                                "description": "Searches AWS documentation using the official AWS Documentation Search API.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "search_phrase": {
                                            "type": "string",
                                            "description": "The phrase to search for in AWS documentation"
                                        },
                                        "limit": {
                                            "type": "integer",
                                            "description": "Maximum number of results to return"
                                        }
                                    },
                                    "required": ["search_phrase"]
                                }
                            },
                            {
                                "name": "read_documentation",
                                "description": "Fetches an AWS documentation page and converts it to markdown format.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "url": {
                                            "type": "string",
                                            "description": "The URL of the AWS documentation page"
                                        }
                                    },
                                    "required": ["url"]
                                }
                            },
                            {
                                "name": "recommend",
                                "description": "Gets content recommendations for an AWS documentation page.",
                                "inputSchema": {
                                    "type": "object",
                                    "properties": {
                                        "url": {
                                            "type": "string",
                                            "description": "The URL of the AWS documentation page"
                                        }
                                    },
                                    "required": ["url"]
                                }
                            }
                        ]
                    }
                }
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
            
            elif request.get("method") == "call_tool" and request.get("params", {}).get("name") == "search_documentation":
                search_phrase = request.get("params", {}).get("arguments", {}).get("search_phrase", "")
                response = {
                    "id": request.get("id"),
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Search results for '{search_phrase}':\n\n1. [Amazon S3 bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)\n2. [Amazon S3 bucket restrictions and limitations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/BucketRestrictions.html)\n3. [Working with Amazon S3 buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html)"
                            }
                        ]
                    }
                }
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
            
            elif request.get("method") == "call_tool" and request.get("params", {}).get("name") == "read_documentation":
                url = request.get("params", {}).get("arguments", {}).get("url", "")
                response = {
                    "id": request.get("id"),
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"# AWS Documentation for {url}\n\nThis is a placeholder for the actual documentation content that would be fetched from the URL."
                            }
                        ]
                    }
                }
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
            
            elif request.get("method") == "call_tool" and request.get("params", {}).get("name") == "recommend":
                url = request.get("params", {}).get("arguments", {}).get("url", "")
                response = {
                    "id": request.get("id"),
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": f"Recommendations for {url}:\n\n1. [Amazon S3 bucket naming rules](https://docs.aws.amazon.com/AmazonS3/latest/userguide/bucketnamingrules.html)\n2. [Amazon S3 bucket restrictions and limitations](https://docs.aws.amazon.com/AmazonS3/latest/userguide/BucketRestrictions.html)\n3. [Working with Amazon S3 buckets](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingBucket.html)"
                            }
                        ]
                    }
                }
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
            
            else:
                response = {
                    "id": request.get("id"),
                    "error": {
                        "code": -32601,
                        "message": f"Method not found: {request.get('method')}"
                    }
                }
                sys.stdout.write(json.dumps(response) + "\n")
                sys.stdout.flush()
                
        except Exception as e:
            error_response = {
                "id": request.get("id") if "request" in locals() else None,
                "error": {
                    "code": -32603,
                    "message": f"Internal error: {str(e)}"
                }
            }
            sys.stdout.write(json.dumps(error_response) + "\n")
            sys.stdout.flush()

if __name__ == "__main__":
    main()
