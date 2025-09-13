from flask import Flask
from utils.neo4jfactory import Neo4jConnection
from utils.loggerfactory import LoggerFactory
import os

def create_app():
    app = Flask(__name__)
    #initialize Logger
    logger_factory = LoggerFactory()
    logger = logger_factory.get_logger("app_logger")
    app.config["LOGGER"] = logger

    from dotenv import load_dotenv
    load_dotenv()
    
    DD_ENV = os.getenv('DD_ENV', 'qa')
    if DD_ENV == "dev_local":
        NEO4J_USERNAME = os.getenv('NEO4J_USERNAME', 'neo4j')
        NEO4j_PASSWORD = os.getenv('NEO4J_PASSWORD', '')
        OKTA_API_TOKEN = os.getenv('OKTA_API_TOKEN', '')
    
    logger.info(f"Creating the  application object for Rbac-{DD_ENV} environment.")
    app.config.from_pyfile(f'config/config_{DD_ENV}.py')
    NEO4J_URI = app.config.get("NEO4J_URI")
    with app.app_context():
        try:
            logger.info("Logger Initialized")
            # Initialize Neo4j driver
            neo4j_factory = Neo4jConnection(
                uri=app.config.get("NEO4J_URI"),
                user=NEO4J_USERNAME,
                password=NEO4j_PASSWORD
            )
            # driver = neo4j_factory.driver
            logger.info(f"Neo4j intialised")

            # Add instances to app config
            app.config["NEO4J"] = neo4j_factory

            # Import and register blueprints
            from .routes import bp as main_bp
            app.register_blueprint(main_bp)

            return app
        except Exception as e:
            logger.info(f"An unexpected error occurred while loading the application object .error is  {e}")
            raise Exception(e)