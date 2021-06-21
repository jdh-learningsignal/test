from flask import Blueprint, current_app, session, request, render_template, flash, redirect, url_for, Markup
import json
from datetime import datetime
import logging

from sres.auth import login_required, is_user_administrator, get_auth_user
from sres.tags import Tag, get_all_tags
from sres.tag_groups import TagGroup, get_all_tag_groups

bp = Blueprint('tag_groups', __name__, url_prefix='/tag_groups')

@bp.route('/<tag_group_uuid>', methods=['GET','POST'])
@login_required
def view_tag_group(tag_group_uuid=None):
    
    vars = {}
    
    if request.method == 'POST':
        # only admins can edit tags
        if not is_user_administrator('list') or is_user_administrator('super'):
            flash("Only administrators are permitted to complete this action.", 'danger')
            return render_template('denied.html')
        # continue
        another_tag_group = TagGroup()
        if is_user_administrator('super') or another_tag_group.is_user_creator(tag_group_uuid):
            another_tag_group.update_tag_group_name(tag_group_uuid=tag_group_uuid, tag_group_name=request.form.get('name'))
            flash('Updated tag name to: ' + request.form.get('name'), 'success')
        else:
            flash("You are not authorised to complete this action", 'danger')

    tag_group = TagGroup()
    if tag_group_uuid is not None:
        tag_group.load_from_uuid(tag_group_uuid)
        
        vars['is_user_creator_of_tag_group'] = tag_group.is_user_creator()
        vars['is_user_authorised_editor'] = vars['is_user_creator_of_tag_group'] or is_user_administrator('super')
        vars['user_can_create_tags'] = is_user_administrator('list') or is_user_administrator('super')
        
        another_tag_group = TagGroup()
        tags = another_tag_group.get_tags(tag_group._id)
    
    return render_template('tag-group-view.html', vars=vars, tag_group=tag_group.config, tags=tags)
