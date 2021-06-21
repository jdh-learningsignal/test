from flask import Blueprint, current_app, session, request, render_template, flash, redirect, url_for, Markup
import json
from datetime import datetime
import logging

from sres.auth import login_required, is_user_administrator, get_auth_user
from sres import utils
from sres.tags import Tag, get_all_tags
from sres.tag_groups import TagGroup, get_all_tag_groups
from sres.access_logs import add_access_event

bp = Blueprint('tags', __name__, url_prefix='/tags')

@bp.route('/<tag_uuid>', methods=['GET','POST'])
@login_required
def view_tag(tag_uuid=None):
    add_access_event(asset_type='tag', asset_uuid=tag_uuid, action='view')
    vars = {}
    if tag_uuid is not None:
        if request.method == 'POST':
            # only admins can edit tags
            if not is_user_administrator('list') and not is_user_administrator('super'):
                flash("Only administrators are permitted to complete this action.", 'danger')
                return render_template('denied.html')
            # continue
            another_tag = Tag()
            if request.form.get('action') == 'change_tag_group':
                if is_user_administrator('super') or another_tag.is_user_creator(tag_uuid):
                    another_tag.update_tag_group(tag_group_objectid=request.form.get('tag_group'), tag_uuid=tag_uuid)
                    flash('Changed tag group','success')
                else:
                    flash("You are not authorised to complete this action", 'danger')
            else: # change_tag_name
                if is_user_administrator('super') or another_tag.is_user_creator(tag_uuid):
                    another_tag.update_tag_name(tag_uuid=tag_uuid, tag_name=request.form.get('name'))
                    flash('Changed tag name to: ' + request.form.get('name'),'success')
                else:
                    flash("You are not authorised to complete this action", 'danger')
        
        tag = Tag()
        if tag.load(tag_uuid):
            vars['is_user_creator_of_tag'] = tag.is_user_creator()
            vars['is_user_authorised_editor'] = vars['is_user_creator_of_tag'] or is_user_administrator('super')
            if tag.config.get('tag_group_id'):
                tag_group = TagGroup()
                tag_group = tag_group.load_from_objectid(tag.config['tag_group_id'])
                if tag_group:
                    vars['tag_group'] = tag_group
                
            tag_groups = get_all_tag_groups()
            
            yet_another_tag = Tag()
            vars['tag_used_by_any_columns'] = False
            if yet_another_tag.num_columns_with_this_tag(tag_uuid=tag._id) > 0:
                vars['tag_used_by_any_columns'] = True
            
            return render_template('tag-view.html', tag=tag.config, vars=vars, tag_groups=tag_groups)
        else:
            flash("Could not load resource.", "danger")
            return render_template('denied.html')

@bp.route('/', methods=['GET', 'POST'])
@login_required
def tags():
    add_access_event(asset_type='tag', asset_uuid=None, action='view')
    if request.method == 'POST':
        # only admins can manipulate tags
        if not is_user_administrator('list') and not is_user_administrator('super'):
            flash("Only administrators are permitted to complete this action.", 'danger')
            return render_template('denied.html')
        if request.form.get('action') == 'tag': # create new tag
            new_tag = Tag()
            new_tag.create(request.form.get('name'), tag_group_objectid=request.form.get('tag_group_objectid'))
            flash('Added new tag: ' + request.form.get('name'),'success')
        elif request.form.get('action') == 'delete_tag':
            tag_to_delete = Tag()
            if is_user_administrator('super') or tag_to_delete.is_user_creator(request.form.get('tag_uuid')):
                tag_to_delete.delete(tag_uuid=request.form.get('tag_uuid'))
                flash('Deleted tag: ' + request.form.get('name'),'warning')
            else:
                flash("You are not authorised to complete this action", 'danger')
        elif request.form.get('action') == 'delete_tag_group':
            tag_group_to_delete = TagGroup()
            if is_user_administrator('super') or tag_group_to_delete.is_user_creator(request.form.get('tag_group_uuid')):
                tag_group_to_delete.delete(tag_group_uuid=request.form.get('tag_group_uuid'))
                flash('Deleted tag group: ' + request.form.get('name'),'warning')
            else:
                flash("You are not authorised to complete this action", 'danger')
        else:                                   # create new tag group
            new_tag_group = TagGroup()
            new_tag_group.create(request.form.get('name'))
            flash('Added new tag group: ' + request.form.get('name'),'success')
    
    tags = get_all_tags()
    tag_groups = get_all_tag_groups()
    
    vars = {'group': {}, 'num_columns': {}}
    vars['user_can_create_tags'] = is_user_administrator('list') or is_user_administrator('super')
    
    for tag in tags:
        if 'tag_group_id' in tag:
            tag_group = TagGroup()
            tag_group = tag_group.load_from_objectid(tag['tag_group_id'])
            if tag_group:
                vars['group'][tag['uuid']] = tag_group
        another_tag = Tag()
        vars['num_columns'][tag['name']] = another_tag.num_columns_with_this_tag(tag['_id'])
        
    return render_template('tags.html', tags=tags, tag_groups=tag_groups, vars=vars)
       

