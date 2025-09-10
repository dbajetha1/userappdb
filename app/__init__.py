from flask import Flask
from utils.neo4jfactory import Neo4jConnection
from utils.loggerfactory import LoggerFactory
def create_app():
    app = Flask(__name__)
    #initialize Logger
    logger_factory = LoggerFactory()
    logger = logger_factory.get_logger("app_logger")
    logger.info("Logger Initialized")
    # Initialize Neo4j driver
    neo4j_factory = Neo4jConnection(
        uri="neo4j://127.0.0.1:7687",
        user="neo4j",
        password="root1234"
    )
    # driver = neo4j_factory.driver
    logger.info(f"Neo4j intialised")

    # Add instances to app config
    app.config["LOGGER"] = logger
    app.config["NEO4J"] = neo4j_factory

    # Import and register blueprints
    from .routes import bp as main_bp
    app.register_blueprint(main_bp)

    return app