import json
from neo4j import GraphDatabase


# Load dummy data
with open("dummydata/oktausers.json") as f:
    users = json.load(f)

with open("dummydata/userapps.json") as f:
    user_apps = json.load(f)


class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def get_session(self):
        return self.driver.session()

def create_user(tx, user):
    query = """
    MERGE (u:User {id: $id})
    SET u.firstName = $firstName, u.lastName = $lastName, u.email = $email
    RETURN u
    """
    tx.run(query, id=user["id"], firstName=user["profile"]["firstName"], lastName=user["profile"]["lastName"], email=user["profile"]["email"])

def create_app(tx, app):
    query = """
    MERGE (a:Application {id: $id})
    SET a.label = $label, a.linkUrl = $linkUrl, a.appName = $appName, a.logoUrl = $logoUrl, a.status = $status, a.signOnMode = $signOnMode
    RETURN a
    """
    tx.run(query, **app)

def assign_app_to_user(tx, user_id, app_id):
    query = """
    MATCH (u:User {id: $user_id}), (a:Application {id: $app_id})
    MERGE (u)-[:USES]->(a)
    """
    tx.run(query, user_id=user_id, app_id=app_id)

def cleanup_users_and_apps(tx, user_ids, app_ids):
    # Remove users not in dummy data
    query_users = """
    MATCH (u:User)
    WHERE NOT u.id IN $user_ids
    DETACH DELETE u
    """
    tx.run(query_users, user_ids=user_ids)

    # Remove apps not in dummy data
    query_apps = """
    MATCH (a:Application)
    WHERE NOT a.id IN $app_ids
    DETACH DELETE a
    """
    tx.run(query_apps, app_ids=app_ids)

def cleanup_relationships(tx, user_apps):
    # Remove relationships not in dummy data
    query = """
    MATCH (u:User)-[r:USES]->(a:Application)
    WHERE NOT (u.id IN $user_ids AND a.id IN $apps_for_user[u.id])
    DELETE r
    """
    # Prepare apps_for_user as a dictionary mapping user_id to list of app_ids
    tx.run(query, user_ids=list(user_apps.keys()), apps_for_user=user_apps)

def remove_duplicate_nodes(tx, label, id_field):
    query = f"""
    MATCH (n:{label})
    WITH n.{id_field} AS id, collect(n) AS nodes
    WHERE size(nodes) > 1
    FOREACH (n IN nodes[1..] | DETACH DELETE n)
    """
    tx.run(query)

# Initialize Neo4j connection
neo4j_conn = Neo4jConnection("neo4j://0.0.0.0:7687", "neo4j", "your-strong-password")

# Prepare lists of IDs
user_ids = [user["id"] for user in users]
app_ids = []
for apps in user_apps.values():
    app_ids.extend([app["id"] for app in apps])
app_ids = list(set(app_ids))  # Remove duplicates

# Prepare mapping for relationships
apps_for_user = {user_id: [app["id"] for app in apps] for user_id, apps in user_apps.items()}

with neo4j_conn.get_session() as session:
    # Remove duplicate User nodes
    session.execute_write(remove_duplicate_nodes, "User", "id")
    # Remove duplicate Application nodes
    session.execute_write(remove_duplicate_nodes, "Application", "id")

    # Clean up users and apps
    session.execute_write(cleanup_users_and_apps, user_ids, app_ids)
    # Clean up relationships
    for user_id, app_list in apps_for_user.items():
        query = """
        MATCH (u:User {id: $user_id})-[r:USES]->(a:Application)
        WHERE NOT a.id IN $app_list
        DELETE r
        """
        session.execute_write(lambda tx: tx.run(query, user_id=user_id, app_list=app_list))

    # Create or update users and apps, and relationships
    for user in users:
        session.execute_write(create_user, user)
        apps = user_apps.get(user["id"], [])
        for app in apps:
            session.execute_write(create_app, app)
            session.execute_write(assign_app_to_user, user["id"], app["id"])

neo4j_conn.close()
print("Cleanup and creation completed successfully.")