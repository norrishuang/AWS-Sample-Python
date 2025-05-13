def fetch_index_mapping(client, index_name):
    """
    Fetch the index mapping to determine the vector field configuration.
    
    Args:
        client: OpenSearch client
        index_name: Name of the index
    
    Returns:
        Dictionary with index configuration details
    """
    try:
        mapping = client.indices.get_mapping(index=index_name)
        properties = mapping[index_name]['mappings'].get('properties', {})
        
        # Look for vector field configuration
        vector_field = properties.get('content_vector', {})
        
        # Extract space type if available
        space_type = 'innerproduct'  # Default
        if 'method' in vector_field:
            space_type = vector_field['method'].get('space_type', 'innerproduct')
        
        print(f"Detected vector field configuration: {vector_field}")
        print(f"Using space type: {space_type}")
        
        return {
            'space_type': space_type
        }
    except Exception as e:
        print(f"Warning: Could not fetch index mapping: {e}")
        print("Using default space type: innerproduct")
        return {
            'space_type': 'innerproduct'
        }
