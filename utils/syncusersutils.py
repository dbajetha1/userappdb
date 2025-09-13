# Neo4j transaction functions
def create_or_update_user(tx, user):
    """Create or update a user node with all profile information"""
    query = """
    MERGE (u:User {id: $id})
    SET u.firstName = $firstName, 
        u.lastName = $lastName, 
        u.email = $email,
        u.login = $login,
        u.status = $status,
        u.created = $created,
        u.lastLogin = $lastLogin,
        u.lastUpdated = $lastUpdated,
        u.type = 'User'
    RETURN u
    """
    profile = user.get("profile", {})
    tx.run(query, 
           id=user["id"],
           firstName=profile.get("firstName", ""),
           lastName=profile.get("lastName", ""),
           email=profile.get("email", ""),
           login=profile.get("login", ""),
           status=user.get("status", ""),
           created=user.get("created", ""),
           lastLogin=user.get("lastLogin", ""),
           lastUpdated=user.get("lastUpdated", ""))

def create_or_update_app(tx, app):
    """Create or update an application node with all details"""
    query = """
    MERGE (a:Application {id: $id})
    SET a.label = $label, 
        a.linkUrl = $linkUrl, 
        a.appName = $appName, 
        a.logoUrl = $logoUrl, 
        a.status = $status, 
        a.signOnMode = $signOnMode,
        a.appInstanceId = $appInstanceId,
        a.sortOrder = $sortOrder,
        a.type = 'Application'
    RETURN a
    """
    tx.run(query,
           id=app.get("id", ""),
           label=app.get("label", ""),
           linkUrl=app.get("linkUrl", ""),
           appName=app.get("appName", ""),
           logoUrl=app.get("logoUrl", ""),
           status=app.get("status", ""),
           signOnMode=app.get("signOnMode", ""),
           appInstanceId=app.get("appInstanceId", ""),
           sortOrder=app.get("sortOrder", 0))

def assign_app_to_user(tx, user_id, app_id):
    """Create USES relationship between user and application"""
    query = """
    MATCH (u:User {id: $user_id}), (a:Application {id: $app_id})
    MERGE (u)-[r:USES]->(a)
    SET r.assignedDate = datetime()
    RETURN r
    """
    tx.run(query, user_id=user_id, app_id=app_id)

def cleanup_users_and_apps(tx, user_ids, app_ids):
    """Remove users and apps not in the current Okta data"""
    # Remove users not in Okta
    query_users = """
    MATCH (u:User)
    WHERE NOT u.id IN $user_ids
    DETACH DELETE u
    """
    result_users = tx.run(query_users, user_ids=user_ids)
    
    # Remove apps not in Okta
    query_apps = """
    MATCH (a:Application)
    WHERE NOT a.id IN $app_ids
    DETACH DELETE a
    """
    result_apps = tx.run(query_apps, app_ids=app_ids)

def cleanup_user_relationships(tx, user_id, app_list):
    """Remove relationships for apps no longer assigned to user"""
    query = """
    MATCH (u:User {id: $user_id})-[r:USES]->(a:Application)
    WHERE NOT a.id IN $app_list
    DELETE r
    """
    tx.run(query, user_id=user_id, app_list=app_list)

def remove_duplicate_nodes(tx, label, id_field):
    """Remove duplicate nodes based on ID field"""
    query = f"""
    MATCH (n:{label})
    WITH n.{id_field} AS id, collect(n) AS nodes
    WHERE size(nodes) > 1
    FOREACH (n IN nodes[1..] | DETACH DELETE n)
    """
    tx.run(query)