from flask import (Blueprint, flash, g, redirect, render_template, request, session, url_for)

from sres.auth import login_required

bp = Blueprint("vis", __name__, url_prefix="/vis")

@bp.route("/new", methods=["GET"])
def new_vis(table_uuid=None):
    
    pass

