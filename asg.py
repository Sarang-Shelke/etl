"""
Abstract Syntax Graph (ASG) implementation for DataStage jobs.

This module provides the ASG class and utilities for working with
the graph representation of DataStage jobs.
"""
from typing import Dict, List, Optional, Set, Tuple
from collections import deque

from schemas import (
    AbstractSyntaxGraph,
    ASGNode,
    ASGEdge,
    Stage,
    Job,
    EdgeType,
    StageType
)
from exceptions import InvalidGraphStructureError


class ASG(AbstractSyntaxGraph):
    """
    Enhanced Abstract Syntax Graph with graph analysis capabilities.
    
    Inherits from AbstractSyntaxGraph and adds graph algorithms and utilities.
    """
    
    def get_topological_order(self) -> List[str]:
        """
        Get nodes in topological order using Kahn's algorithm.
        
        Returns:
            List of node IDs in topological order
        
        Raises:
            InvalidGraphStructureError: If graph contains cycles
        """
        # Calculate in-degrees
        in_degree: Dict[str, int] = {node_id: 0 for node_id in self.nodes}
        for edge in self.edges:
            in_degree[edge.target_id] += 1
        
        # Find nodes with no incoming edges
        queue = deque([node_id for node_id, degree in in_degree.items() if degree == 0])
        result = []
        
        while queue:
            node_id = queue.popleft()
            result.append(node_id)
            
            # Reduce in-degree for neighbors
            for neighbor in self.adjacency_list.get(node_id, []):
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)
        
        # Check for cycles
        if len(result) != len(self.nodes):
            raise InvalidGraphStructureError(
                "Graph contains cycles - cannot determine topological order"
            )
        
        return result
    
    def detect_cycles(self) -> List[List[str]]:
        """
        Detect all cycles in the graph using DFS.
        
        Returns:
            List of cycles, where each cycle is a list of node IDs
        """
        cycles = []
        visited: Set[str] = set()
        rec_stack: Set[str] = set()
        path: List[str] = []
        
        def dfs(node_id: str, parent: Optional[str] = None) -> None:
            visited.add(node_id)
            rec_stack.add(node_id)
            path.append(node_id)
            
            for neighbor in self.adjacency_list.get(node_id, []):
                if neighbor not in visited:
                    dfs(neighbor, node_id)
                elif neighbor in rec_stack:
                    # Cycle detected
                    cycle_start = path.index(neighbor)
                    cycle = path[cycle_start:] + [neighbor]
                    cycles.append(cycle.copy())
            
            rec_stack.remove(node_id)
            path.pop()
        
        for node_id in self.nodes:
            if node_id not in visited:
                dfs(node_id)
        
        return cycles
    
    def get_roots(self) -> List[str]:
        """Get all root nodes (nodes with no incoming edges)."""
        nodes_with_incoming = {edge.target_id for edge in self.edges}
        return [node_id for node_id in self.nodes if node_id not in nodes_with_incoming]
    
    def get_leaves(self) -> List[str]:
        """Get all leaf nodes (nodes with no outgoing edges)."""
        nodes_with_outgoing = {edge.source_id for edge in self.edges}
        return [node_id for node_id in self.nodes if node_id not in nodes_with_outgoing]
    
    def get_paths(self, source_id: str, target_id: str, max_depth: Optional[int] = None) -> List[List[str]]:
        """
        Find all paths from source to target node.
        
        Args:
            source_id: Source node ID
            target_id: Target node ID
            max_depth: Maximum path depth (None for unlimited)
        
        Returns:
            List of paths, where each path is a list of node IDs
        """
        if source_id not in self.nodes or target_id not in self.nodes:
            return []
        
        paths = []
        
        def dfs(current: str, path: List[str], depth: int) -> None:
            if current == target_id:
                paths.append(path.copy())
                return
            
            if max_depth is not None and depth >= max_depth:
                return
            
            for neighbor in self.adjacency_list.get(current, []):
                if neighbor not in path:  # Avoid cycles
                    path.append(neighbor)
                    dfs(neighbor, path, depth + 1)
                    path.pop()
        
        dfs(source_id, [source_id], 0)
        return paths
    
    def get_subgraph(self, node_ids: Set[str]) -> 'ASG':
        """
        Extract a subgraph containing only specified nodes and their edges.
        
        Args:
            node_ids: Set of node IDs to include
        
        Returns:
            New ASG instance containing the subgraph
        """
        subgraph = ASG()
        
        # Add nodes
        for node_id in node_ids:
            if node_id in self.nodes:
                subgraph.add_node(self.nodes[node_id])
        
        # Add edges between included nodes
        for edge in self.edges:
            if edge.source_id in node_ids and edge.target_id in node_ids:
                subgraph.add_edge(edge)
        
        return subgraph
    
    def get_levels(self) -> Dict[str, int]:
        """
        Calculate the level (depth from root) of each node.
        
        Returns:
            Dictionary mapping node ID to level
        """
        levels: Dict[str, int] = {}
        roots = self.get_roots()
        
        # Initialize levels
        for node_id in self.nodes:
            if node_id in roots:
                levels[node_id] = 0
            else:
                levels[node_id] = -1  # Unvisited
        
        # BFS from roots
        queue = deque(roots)
        while queue:
            current = queue.popleft()
            current_level = levels[current]
            
            for neighbor in self.adjacency_list.get(current, []):
                if levels[neighbor] == -1 or levels[neighbor] > current_level + 1:
                    levels[neighbor] = current_level + 1
                    queue.append(neighbor)
        
        # Handle unvisited nodes (in cycles or disconnected)
        for node_id in self.nodes:
            if levels[node_id] == -1:
                levels[node_id] = 0
        
        return levels
    
    def to_dict(self) -> Dict:
        """Convert ASG to dictionary representation."""
        return {
            "nodes": {
                node_id: {
                    "node_id": node.node_id,
                    "node_type": node.node_type,
                    "stage_name": node.stage.name,
                    "stage_type": node.stage.stage_type.value,
                    "properties": node.properties,
                    "level": node.level
                }
                for node_id, node in self.nodes.items()
            },
            "edges": [
                {
                    "source_id": edge.source_id,
                    "target_id": edge.target_id,
                    "edge_type": edge.edge_type.value,
                    "properties": edge.properties,
                    "weight": edge.weight
                }
                for edge in self.edges
            ],
            "metadata": self.metadata
        }
    
    @classmethod
    def from_job(cls, job: Job) -> 'ASG':
        """
        Build an ASG from a Job object.
        
        Args:
            job: The Job object to convert
        
        Returns:
            ASG instance
        """
        asg = cls()
        asg.metadata["job_name"] = job.job_name
        asg.metadata["description"] = job.description
        
        # Add all stages as nodes
        levels = {}  # Will be calculated after edges are added
        
        for stage in job.stages:
            node = ASGNode(
                node_id=stage.id or stage.name,
                node_type=stage.stage_type.value,
                stage=stage,
                properties=stage.properties.custom_properties.copy()
            )
            asg.add_node(node)
        
        # Add all links as edges
        for link in job.links:
            edge = ASGEdge(
                source_id=link.source_stage,
                target_id=link.target_stage,
                edge_type=link.edge_type,
                properties=link.link_properties.copy()
            )
            asg.add_edge(edge)
        
        # Calculate and assign levels
        levels = asg.get_levels()
        for node_id, level in levels.items():
            if node_id in asg.nodes:
                asg.nodes[node_id].level = level
        
        return asg
