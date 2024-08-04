from fastapi import FastAPI, Form
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
import json
from typing import List, Dict,Tuple

app = FastAPI()

# Allow all origins (for development purposes)
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # Allow all methods (GET, POST, etc.)
    allow_headers=["*"],  # Allow all headers
)



def is_dag(nodes: List[Dict], edges: List[Dict]) -> Tuple[bool, List[str], List[Tuple[str, str]]]:
    graph = defaultdict(list)
    
    # Build the graph from edges
    for edge in edges:
        graph[edge['source']].append(edge['target'])
    
    # Add all nodes to the graph if they are not already present (including isolated nodes)
    for node in nodes:
        if node['id'] not in graph:
            graph[node['id']] = []

    def dfs(node, visited, rec_stack, cycle_nodes, cycle_edges):
        visited.add(node)
        rec_stack.add(node)
        
        for neighbor in graph[node]:
            if neighbor not in visited:
                if dfs(neighbor, visited, rec_stack, cycle_nodes, cycle_edges):
                    cycle_edges.append((node, neighbor))
                    cycle_nodes.add(neighbor)
                    return True
            elif neighbor in rec_stack:
                cycle_edges.append((node, neighbor))
                cycle_nodes.add(neighbor)
                return True
        
        rec_stack.remove(node)
        return False
    
    visited = set()
    rec_stack = set()
    cycle_nodes = set()
    cycle_edges = []
    
    for node in graph.keys():
        if node not in visited:
            if dfs(node, visited, rec_stack, cycle_nodes, cycle_edges):
                cycle_nodes.add(node)
    
    is_dag = len(cycle_nodes) == 0
    return is_dag, list(cycle_nodes), cycle_edges

@app.get('/')
def read_root():
    return {'Ping': 'Pong'}

@app.post('/pipelines/parse')
def parse_pipeline(pipeline: str = Form(...)):
    try:
        # Parse the JSON string from the form data
        data = json.loads(pipeline)
        nodes = data.get('nodesData', [])
        edges = data.get('edgesData', [])
        
        response=is_dag(nodes,edges)
        
        return {'status': 'parsed', 'result': {'num_nodes':len(response[1]),'num_edges':len(response[2]),'is_dag':response[0]}}
    except json.JSONDecodeError:
        return {'status': 'error', 'message': 'Invalid JSON format'}
    except Exception as e:
        return {'status': 'error', 'message': str(e)}
