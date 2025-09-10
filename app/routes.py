import json
from flask import Blueprint, current_app

bp = Blueprint("main", __name__)

@bp.route("/")
def index():
    return "Flask (factory pattern) is running inside Docker!"

@bp.route("/useraccess")
def user_access():
    

@bp.route("/syncusers")
def sync_users():
    # Load dummy data
    with open("dummydata/oktausers.json") as f:
        users = json.load(f)

    with open("dummydata/userapps.json") as f:
        user_apps = json.load(f)
    # Get instances from app config
    neo4j_conn = current_app.config["NEO4J"]
    logger = current_app.config["LOGGER"]

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
        session.execute_write(neo4j_conn.remove_duplicate_nodes, "User", "id")
        # Remove duplicate Application nodes
        session.execute_write(neo4j_conn.remove_duplicate_nodes, "Application", "id")

        # Clean up users and apps
        session.execute_write(neo4j_conn.cleanup_users_and_apps, user_ids, app_ids)
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
            session.execute_write(neo4j_conn.create_user, user)
            apps = user_apps.get(user["id"], [])
            for app in apps:
                session.execute_write(neo4j_conn.create_app, app)
                session.execute_write(neo4j_conn.assign_app_to_user, user["id"], app["id"])

    neo4j_conn.close()
    logger.info("Cleanup and creation completed successfully.")
    return "User and Application data synchronized successfully!"