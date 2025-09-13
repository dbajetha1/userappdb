import json
from flask import Blueprint, current_app
import os
from utils.okta_factory import OktaFactory
from utils.syncusersutils import assign_app_to_user, cleanup_user_relationships, cleanup_users_and_apps, create_or_update_app, create_or_update_user, remove_duplicate_nodes
bp = Blueprint("main", __name__)

@bp.route("/")
def index():
    return "Flask (factory pattern) is running inside Docker!"

@bp.route("/syncusers")
def sync_users():
    logger = None
    neo4j_conn = None
    okta_factory = None
    
    try:
        # Get instances from app config
        neo4j_conn = current_app.config["NEO4J"]
        logger = current_app.config["LOGGER"]
        
        logger.info("Starting user synchronization process")
        
        # Initialize Okta factory
        okta_base_url = current_app.config.get("OKTA_BASE_URL")
        okta_api_token = os.getenv('OKTA_API_TOKEN', '')
        
        if not okta_base_url or not okta_api_token:
            raise ValueError("OKTA_BASE_URL or OKTA_API_TOKEN not configured properly")
        
        okta_factory = OktaFactory(okta_base_url, okta_api_token)
        
        # Step 1: Get all active users from Okta
        logger.info("Step 1: Fetching all active users from Okta")
        users = okta_factory.get_all_active_users()
        logger.info(f"Retrieved {len(users)} active users from Okta")
        
        # Step 2: Get apps for those specific users
        logger.info("Step 2: Fetching applications for each user")
        user_apps = okta_factory.get_apps_for_users(users)
        logger.info(f"Retrieved applications for {len(user_apps)} users")
        
        # Prepare data for Neo4j operations
        user_ids = [user["id"] for user in users]
        
        # Collect all unique applications
        all_apps = {}
        for user_id, apps in user_apps.items():
            for app in apps:
                all_apps[app["id"]] = app
        
        app_ids = list(all_apps.keys())
        logger.info(f"Found {len(app_ids)} unique applications")
        
        # Step 3: Create nodes and relationships in Neo4j
        with neo4j_conn.get_session() as session:
            logger.info("Step 3: Starting database synchronization")
            # Remove duplicate nodes first
            logger.info("Removing duplicate User nodes")
            session.write_transaction(remove_duplicate_nodes, "User", "id")

            logger.info("Removing duplicate Application nodes")
            session.write_transaction(remove_duplicate_nodes, "Application", "id")
            
            # Step 4: Remove users not received from Okta
            logger.info("Step 4: Cleaning up users not in Okta")
            session.write_transaction(cleanup_users_and_apps, user_ids, app_ids)
            
            # Clean up old relationships
            logger.info("Cleaning up old relationships")
            for user_id, app_list in user_apps.items():
                app_id_list = [app["id"] for app in app_list]
                session.write_transaction(cleanup_user_relationships, user_id, app_id_list)
            
            # Create or update users
            logger.info("Creating/updating user nodes")
            for user in users:
                session.write_transaction(create_or_update_user, user)
            
            # Create or update applications
            logger.info("Creating/updating application nodes")
            for app in all_apps.values():
                session.write_transaction(create_or_update_app, app)
            
            # Create relationships between users and apps
            logger.info("Creating user-application relationships")
            for user_id, apps in user_apps.items():
                for app in apps:
                    session.write_transaction(assign_app_to_user, user_id, app["id"])
        
        logger.info("User synchronization completed successfully")
        return {
            "status": "success",
            "message": "User and Application data synchronized successfully!",
            "users_processed": len(users),
            "applications_processed": len(app_ids)
        }
        
    except ValueError as ve:
        error_msg = f"Configuration error: {str(ve)}"
        if logger:
            logger.error(error_msg)
        return {"status": "error", "message": error_msg}, 400
        
    except Exception as e:
        error_msg = f"An unexpected error occurred during synchronization: {str(e)}"
        if logger:
            logger.error(error_msg, exc_info=True)
        return {"status": "error", "message": error_msg}, 500
        
    finally:
        # Clean up connections
        if okta_factory:
            okta_factory.close()