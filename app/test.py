from flask import Flask, request, jsonify
from neo4j import GraphDatabase
import os
from datetime import datetime

app = Flask(__name__)

# Neo4j connection configuration
NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def get_session(self):
        return self.driver.session()

# Initialize Neo4j connection
neo4j_conn = Neo4jConnection(NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD)

# Helper functions for Neo4j operations
def create_node(tx, label, properties):
    query = f"CREATE (n:{label} $props) RETURN n"
    result = tx.run(query, props=properties)
    return result.single()[0]

def create_relationship(tx, from_node_id, to_node_id, rel_type, properties=None):
    if properties:
        query = f"""
        MATCH (a), (b)
        WHERE id(a) = $from_id AND id(b) = $to_id
        CREATE (a)-[r:{rel_type} $props]->(b)
        RETURN r
        """
        result = tx.run(query, from_id=from_node_id, to_id=to_node_id, props=properties)
    else:
        query = f"""
        MATCH (a), (b)
        WHERE id(a) = $from_id AND id(b) = $to_id
        CREATE (a)-[r:{rel_type}]->(b)
        RETURN r
        """
        result = tx.run(query, from_id=from_node_id, to_id=to_node_id)
    return result.single()[0]

# API Routes
@app.route('/')
def home():
    return jsonify({
        "message": "Flask Neo4j API",
        "endpoints": {
            "POST /person": "Create a person node",
            "POST /company": "Create a company node",
            "POST /relationship": "Create a relationship between nodes",
            "GET /people": "Get all people",
            "GET /companies": "Get all companies",
            "POST /example": "Create example graph"
        }
    })

@app.route('/person', methods=['POST'])
def create_person():
    """Create a Person node"""
    try:
        data = request.json
        name = data.get('name')
        age = data.get('age')
        email = data.get('email')
        
        if not name:
            return jsonify({"error": "Name is required"}), 400
        
        with neo4j_conn.get_session() as session:
            result = session.write_transaction(
                lambda tx: tx.run(
                    "CREATE (p:Person {name: $name, age: $age, email: $email, created_at: $created_at}) RETURN p, id(p) as node_id",
                    name=name,
                    age=age,
                    email=email,
                    created_at=datetime.now().isoformat()
                ).single()
            )
            
            person = dict(result['p'])
            person['node_id'] = result['node_id']
            
            return jsonify({
                "message": "Person created successfully",
                "person": person
            }), 201
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/company', methods=['POST'])
def create_company():
    """Create a Company node"""
    try:
        data = request.json
        name = data.get('name')
        industry = data.get('industry')
        founded_year = data.get('founded_year')
        
        if not name:
            return jsonify({"error": "Company name is required"}), 400
        
        with neo4j_conn.get_session() as session:
            result = session.write_transaction(
                lambda tx: tx.run(
                    "CREATE (c:Company {name: $name, industry: $industry, founded_year: $founded_year, created_at: $created_at}) RETURN c, id(c) as node_id",
                    name=name,
                    industry=industry,
                    founded_year=founded_year,
                    created_at=datetime.now().isoformat()
                ).single()
            )
            
            company = dict(result['c'])
            company['node_id'] = result['node_id']
            
            return jsonify({
                "message": "Company created successfully",
                "company": company
            }), 201
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/relationship', methods=['POST'])
def create_relationship_endpoint():
    """Create a relationship between two nodes"""
    try:
        data = request.json
        from_name = data.get('from_name')
        to_name = data.get('to_name')
        rel_type = data.get('relationship_type', 'WORKS_FOR')
        properties = data.get('properties', {})
        
        if not from_name or not to_name:
            return jsonify({"error": "Both from_name and to_name are required"}), 400
        
        with neo4j_conn.get_session() as session:
            # Create relationship using names
            query = f"""
            MATCH (a {{name: $from_name}}), (b {{name: $to_name}})
            CREATE (a)-[r:{rel_type} $props]->(b)
            RETURN a, b, r, type(r) as rel_type
            """
            
            result = session.write_transaction(
                lambda tx: tx.run(query, from_name=from_name, to_name=to_name, props=properties).single()
            )
            
            if not result:
                return jsonify({"error": "Could not find nodes with given names"}), 404
            
            return jsonify({
                "message": "Relationship created successfully",
                "from": dict(result['a']),
                "to": dict(result['b']),
                "relationship": {
                    "type": result['rel_type'],
                    "properties": dict(result['r'])
                }
            }), 201
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/people', methods=['GET'])
def get_all_people():
    """Get all Person nodes"""
    try:
        with neo4j_conn.get_session() as session:
            result = session.read_transaction(
                lambda tx: tx.run("MATCH (p:Person) RETURN p, id(p) as node_id").data()
            )
            
            people = []
            for record in result:
                person = dict(record['p'])
                person['node_id'] = record['node_id']
                people.append(person)
            
            return jsonify({
                "count": len(people),
                "people": people
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/companies', methods=['GET'])
def get_all_companies():
    """Get all Company nodes"""
    try:
        with neo4j_conn.get_session() as session:
            result = session.read_transaction(
                lambda tx: tx.run("MATCH (c:Company) RETURN c, id(c) as node_id").data()
            )
            
            companies = []
            for record in result:
                company = dict(record['c'])
                company['node_id'] = record['node_id']
                companies.append(company)
            
            return jsonify({
                "count": len(companies),
                "companies": companies
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/example', methods=['POST'])
def create_example_graph():
    """Create an example graph with multiple nodes and relationships"""
    try:
        with neo4j_conn.get_session() as session:
            # Clear existing data (optional)
            session.write_transaction(lambda tx: tx.run("MATCH (n) DETACH DELETE n"))
            
            # Create nodes and relationships
            result = session.write_transaction(lambda tx: tx.run("""
                // Create People
                CREATE (john:Person {name: 'John Doe', age: 30, email: 'john@example.com', skills: ['Python', 'Neo4j']})
                CREATE (jane:Person {name: 'Jane Smith', age: 28, email: 'jane@example.com', skills: ['Java', 'GraphQL']})
                CREATE (bob:Person {name: 'Bob Johnson', age: 35, email: 'bob@example.com', skills: ['JavaScript', 'React']})
                
                // Create Companies
                CREATE (techcorp:Company {name: 'TechCorp', industry: 'Technology', founded_year: 2010})
                CREATE (datacorp:Company {name: 'DataCorp', industry: 'Data Analytics', founded_year: 2015})
                
                // Create Projects
                CREATE (proj1:Project {name: 'Graph Database Migration', status: 'Active', budget: 100000})
                CREATE (proj2:Project {name: 'Real-time Analytics', status: 'Planning', budget: 150000})
                
                // Create Relationships
                CREATE (john)-[:WORKS_FOR {since: 2020, position: 'Senior Developer'}]->(techcorp)
                CREATE (jane)-[:WORKS_FOR {since: 2019, position: 'Lead Engineer'}]->(techcorp)
                CREATE (bob)-[:WORKS_FOR {since: 2021, position: 'Data Scientist'}]->(datacorp)
                
                CREATE (john)-[:KNOWS {since: 2018}]->(jane)
                CREATE (jane)-[:KNOWS {since: 2018}]->(john)
                CREATE (jane)-[:KNOWS {since: 2020}]->(bob)
                
                CREATE (john)-[:WORKS_ON {role: 'Developer', hours_per_week: 20}]->(proj1)
                CREATE (jane)-[:MANAGES]->(proj1)
                CREATE (bob)-[:WORKS_ON {role: 'Analyst', hours_per_week: 30}]->(proj2)
                
                CREATE (techcorp)-[:OWNS]->(proj1)
                CREATE (datacorp)-[:OWNS]->(proj2)
                
                RETURN 
                    count(distinct john) + count(distinct jane) + count(distinct bob) as people_count,
                    count(distinct techcorp) + count(distinct datacorp) as company_count,
                    count(distinct proj1) + count(distinct proj2) as project_count
            """).single())
            
            return jsonify({
                "message": "Example graph created successfully",
                "statistics": {
                    "people_created": result['people_count'],
                    "companies_created": result['company_count'],
                    "projects_created": result['project_count']
                }
            }), 201
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/graph', methods=['GET'])
def get_graph_visualization():
    """Get graph data for visualization"""
    try:
        with neo4j_conn.get_session() as session:
            result = session.read_transaction(lambda tx: tx.run("""
                MATCH (n)
                OPTIONAL MATCH (n)-[r]->(m)
                RETURN 
                    collect(distinct {
                        id: id(n), 
                        label: labels(n)[0], 
                        properties: properties(n)
                    }) as nodes,
                    collect(distinct {
                        source: id(n), 
                        target: id(m), 
                        type: type(r),
                        properties: properties(r)
                    }) as relationships
            """).single())
            
            return jsonify({
                "nodes": result['nodes'],
                "relationships": [r for r in result['relationships'] if r['type'] is not None]
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/query', methods=['POST'])
def custom_query():
    """Execute a custom Cypher query"""
    try:
        data = request.json
        query = data.get('query')
        
        if not query:
            return jsonify({"error": "Query is required"}), 400
        
        # Basic safety check (in production, implement proper query validation)
        forbidden_keywords = ['DELETE', 'REMOVE', 'DROP', 'DETACH']
        if any(keyword in query.upper() for keyword in forbidden_keywords):
            return jsonify({"error": "Destructive operations not allowed"}), 403
        
        with neo4j_conn.get_session() as session:
            result = session.read_transaction(
                lambda tx: tx.run(query).data()
            )
            
            return jsonify({
                "result": result,
                "count": len(result)
            })
            
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.teardown_appcontext
def close_db(error):
    """Clean up database connections"""
    pass

if __name__ == '__main__':
    app.run(debug=True, port=5000)