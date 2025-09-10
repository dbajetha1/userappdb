# from neo4j import GraphDatabase
# from threading import Lock

# class Neo4jFactory:
#     _instance = None
#     _lock = Lock()
#     _driver = None

#     def __new__(cls, uri=None, user=None, password=None):
#         if not cls._instance:
#             with cls._lock:
#                 if not cls._instance:
#                     cls._instance = super(Neo4jFactory, cls).__new__(cls)
#                     if uri and user and password:
#                         cls._driver = GraphDatabase.driver(uri, auth=(user, password))
#         return cls._instance

#     def get_driver(self):
#         if not self._driver:
#             raise Exception("Neo4j driver not initialized. Please provide uri, user, and password.")
#         return self._driver

#     @classmethod
#     def close(cls):
#         if cls._driver:
#             cls._driver.close()
#             cls._driver
import json
from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def get_session(self):
        return self.driver.session()
    
    # Methods for creating users, apps, relationships, and cleanup
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

