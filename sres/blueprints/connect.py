from flask import (Blueprint, flash, redirect, url_for, abort, request, render_template)

from sres.connector_canvas import CanvasConnector
from sres.connector_zoom import ZoomConnector
from sres.auth import login_required

bp = Blueprint("connect", __name__, url_prefix="/connect")

@bp.route("/canvas", methods=["GET"])
@login_required
def connect_canvas():
    connector = CanvasConnector()
    if request.args.get('code') and request.args.get('state'):
        auth_result = connector.process_oauth2_response(request.args.get('code'), request.args.get('state'))
        if auth_result['success']:
            return redirect(url_for('table.connect_lms', table_uuid=auth_result['table_uuid'], lms='canvas'))
        else:
            flash("Error in authorisation workflow.", "danger")
            return render_template('denied.html')
    else:
        flash("Unauthorised.", "danger")
        return render_template('denied.html')

@bp.route("/zoom", methods=["GET"])
@login_required
def connect_zoom():
    connector = ZoomConnector()
    if request.args.get('code') and request.args.get('state'):
        auth_result = connector.process_oauth2_response(request.args.get('code'), request.args.get('state'))
        if auth_result['success']:
            return redirect(url_for('table.connect_lms', table_uuid=auth_result['table_uuid'], lms='zoom'))
        else:
            flash("Error in authorisation workflow.", "danger")
            return render_template('denied.html')
    else:
        flash("Unauthorised.", "danger")
        return render_template('denied.html')

