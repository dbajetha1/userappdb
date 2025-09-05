from flask import Blueprint

bp = Blueprint("main", __name__)
print(__name__)

@bp.route("/")
def index():
    return "Flask (factory pattern) is running inside Docker!"