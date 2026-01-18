from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional

app = FastAPI()

# Add CORS middleware to allow frontend connections
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify exact origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request models for validation
class Node(BaseModel):
    id: str
    type: Optional[str] = None
    data: Optional[Dict] = None
    position: Optional[Dict] = None

class Edge(BaseModel):
    id: Optional[str] = None
    source: str
    target: str
    sourceHandle: Optional[str] = None
    targetHandle: Optional[str] = None

class PipelineRequest(BaseModel):
    nodes: List[Node]
    edges: List[Edge]

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse')
async def parse_pipeline(request: PipelineRequest):
    try:
        nodes = request.nodes
        edges = request.edges
        
        num_nodes = len(nodes)
        num_edges = len(edges)
        
        # Handle empty pipeline
        if num_nodes == 0:
            return {
                'num_nodes': 0,
                'num_edges': 0,
                'is_dag': True  # Empty graph is technically a DAG
            }
        
        # Build adjacency list
        node_ids = {node.id for node in nodes}
        adj = {node.id: [] for node in nodes}
        
        # Validate and build edges
        for edge in edges:
            if edge.source not in node_ids:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Edge source '{edge.source}' does not exist in nodes"
                )
            if edge.target not in node_ids:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Edge target '{edge.target}' does not exist in nodes"
                )
            adj[edge.source].append(edge.target)
        
        # Check for DAG using Kahn's algorithm (Topological Sort)
        in_degree = {node.id: 0 for node in nodes}
        
        # Calculate in-degrees
        for node_id in adj:
            for target in adj[node_id]:
                in_degree[target] += 1
        
        # Start with nodes that have no incoming edges
        queue = [node_id for node_id, degree in in_degree.items() if degree == 0]
        visited_count = 0
        
        # Process nodes in topological order
        while queue:
            current = queue.pop(0)
            visited_count += 1
            
            # Reduce in-degree for neighbors
            for neighbor in adj[current]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # If we visited all nodes, it's a DAG (no cycles)
        is_dag = visited_count == num_nodes
        
        return {
            'num_nodes': num_nodes,
            'num_edges': num_edges,
            'is_dag': is_dag
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

# Add a health check endpoint
@app.get('/health')
def health_check():
    return {'status': 'healthy', 'service': 'pipeline-parser'}