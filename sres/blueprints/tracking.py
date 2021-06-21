from flask import Blueprint, url_for, escape, Response, request, render_template, redirect
from bs4 import BeautifulSoup
import base64
from urllib.parse import urlparse

from sres.logs import log_email_url_click, log_email_open, get_send_logs, add_feedback_event

bp = Blueprint('tracking', __name__, url_prefix='/t')

_PIXEL_GIF_DATA = base64.b64decode(b"R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7")
_PIXEL_PNG_DATA = base64.b64decode(b"iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg==")

@bp.route('/b.png', methods=['GET'])
def get_beacon():
    """Tracking beacon requested i.e. email opened"""
    # log it
    try:
        log_uuid = request.args.get('u')
        if log_uuid:
            log_email_open(log_uuid)
    except:
        print('tracking.get_beacon tracking error')
    # return image
    return Response(_PIXEL_PNG_DATA, mimetype='image/png')

@bp.route('/u', methods=['GET'])
def url():
    """A tracked URL has been clicked from an email"""
    # TODO what about for portal clicks - perhaps via portal.py?
    try:
        log_uuid = request.args.get('l') # this log_uuid is for message_send_log not interactions
        url = request.args.get('u')
        if log_uuid:
            log_email_url_click(url=url, log_uuid=log_uuid)
        # see if url has a scheme
        _u = urlparse(url)
        if _u.scheme == '':
            _u = _u._replace(scheme='http')
            url = _u.geturl()
    except:
        print('tracking.url tracking error')
    # redirect
    return redirect(url)

@bp.route('/o', methods=['GET'])
def open():
    pass

@bp.route('/f/<log_uuid>', methods=['GET', 'POST'])
def feedback(log_uuid=None):
    """
        Show or process the feedback form
    """
    if not log_uuid:
        flash("Sorry, not enough information was provided to complete this request.", "warning")
        return render_template('denied.html')
    from sres.filters import Filter
    vars = {}
    vars['log_uuid'] = log_uuid # this is the log_uuid of message_send_log
    log_result = get_send_logs(log_uuid=log_uuid)
    source_asset_type = log_result[0]['source_asset_type'] if log_result else None
    source_asset_uuid = log_result[0]['source_asset_uuid'] if log_result else None
    target = log_result[0]['target'] if log_result else None
    vote = request.args.get('v')
    if request.method == 'GET':
        vars['mode'] = 'get'
        vars['vote'] = vote
        # record the vote
        feedback_log_id = add_feedback_event(
            source_asset_type=source_asset_type,
            source_asset_uuid=source_asset_uuid,
            parent=log_uuid,
            vote=vote,
            data={},
            target=target
        )
        # show the feedback form
        if vote.lower() == 'yes':
            vars['prompt'] = "Thanks for your feedback. How was it helpful?"
        elif vote.lower() == 'no':
            vars['prompt'] = "Sorry it wasn't helpful. How can we improve?"
        else:
            vars['prompt'] = "How was the message helpful or unhelpful?"
        vars['feedback_log_id'] = feedback_log_id
        vars['source_asset_type'] = source_asset_type
        vars['source_asset_uuid'] = source_asset_uuid
    elif request.method == 'POST':
        feedback_log_id = request.form.get('i')
        log_uuid = request.form.get('l')
        source_asset_type = request.form.get('t')
        source_asset_uuid = request.form.get('u')
        comment = request.form.get('c')
        # save comment
        feedback_log_id = add_feedback_event(
            _id=feedback_log_id,
            data={
                'comment': comment
            }
        )
        # notify if necessary
        if source_asset_type == 'filter':
            filter = Filter()
            if filter.load(source_asset_uuid):
                filter.notify_feedback_comment(log_uuid, comment)
        # show thanks
        vars['mode'] = 'post'
    return render_template('feedback.html', vars=vars)


