from flask import Blueprint, flash, render_template, redirect, request, url_for, current_app, abort

from sres.go import encode_go_url, decode_go_code

bp = Blueprint("go", __name__, url_prefix="/go")

@bp.route("/<code>", methods=["GET"])
def go_code(code):
    go_url = decode_go_code(code)
    if go_url:
        return redirect(go_url)
    else:
        flash('Sorry, that link does not appear to be valid.', 'danger')
        return render_template('denied.html')
