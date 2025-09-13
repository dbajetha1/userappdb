import json
from neo4j import GraphDatabase

class Neo4jConnection:
    def __init__(self, uri, user, password):
        print(f"{uri}-{user}-{password}")
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def get_session(self):
        return self.driver.session()
    
    # Methods for creating users, apps, relationships, and cleanup
    def create_user(self, user):
        def _create_user_tx(tx, user):
            query = """
            MERGE (u:User {id: $id})
            SET u.firstName = $firstName, 
                u.lastName = $lastName, 
                u.email = $email,
                u.type = "User"
            RETURN u
            """
            return tx.run(query, 
                         id=user["id"], 
                         firstName=user["profile"]["firstName"], 
                         lastName=user["profile"]["lastName"], 
                         email=user["profile"]["email"])
        
        with self.get_session() as session:
            return session.write_transaction(_create_user_tx, user)

    def create_app(self, app):
        def _create_app_tx(tx, app):
            query = """
            MERGE (a:Application {id: $id})
            SET a.label = $label, 
                a.linkUrl = $linkUrl, 
                a.appName = $appName, 
                a.logoUrl = $logoUrl, 
                a.status = $status, 
                a.signOnMode = $signOnMode,
                a.type = "Application"
            RETURN a
            """
            return tx.run(query, **app)
        
        with self.get_session() as session:
            return session.write_transaction(_create_app_tx, app)

    def assign_app_to_user(self, user_id, app_id):
        def _assign_app_tx(tx, user_id, app_id):
            query = """
            MATCH (u:User {id: $user_id}), (a:Application {id: $app_id})
            MERGE (u)-[:ASSIGNED]->(a)
            """
            return tx.run(query, user_id=user_id, app_id=app_id)
        
        with self.get_session() as session:
            return session.write_transaction(_assign_app_tx, user_id, app_id)

    def cleanup_users_and_apps(self, user_ids, app_ids):
        def _cleanup_tx(tx, user_ids, app_ids):
            # Remove users not in current data
            query_users = """
            MATCH (u:User)
            WHERE NOT u.id IN $user_ids
            DETACH DELETE u
            """
            tx.run(query_users, user_ids=user_ids)

            # Remove apps not in current data
            query_apps = """
            MATCH (a:Application)
            WHERE NOT a.id IN $app_ids
            DETACH DELETE a
            """
            tx.run(query_apps, app_ids=app_ids)
        
        with self.get_session() as session:
            return session.write_transaction(_cleanup_tx, user_ids, app_ids)

    def cleanup_relationships(self, user_apps):
        def _cleanup_rel_tx(tx, user_apps):
            # Remove relationships not in current data
            for user_id, app_ids in user_apps.items():
                query = """
                MATCH (u:User {id: $user_id})-[r:ASSIGNED]->(a:Application)
                WHERE NOT a.id IN $app_ids
                DELETE r
                """
                tx.run(query, user_id=user_id, app_ids=app_ids)
        
        with self.get_session() as session:
            return session.write_transaction(_cleanup_rel_tx, user_apps)
        
    def remove_duplicate_nodes(self, label, id_field):
        def _remove_duplicates_tx(tx, label, id_field):
            query = f"""
            MATCH (n:{label})
            WITH n.{id_field} AS id, collect(n) AS nodes
            WHERE size(nodes) > 1
            FOREACH (n IN nodes[1..] | DETACH DELETE n)
            """
            return tx.run(query)
        
        with self.get_session() as session:
            return session.write_transaction(_remove_duplicates_tx, label, id_field)
